"""
网页抓取与正文提取模块
使用 readability + BeautifulSoup 提取正文，自动剔除广告
针对百度百家号等 JS 渲染页面做了特殊处理
"""

import re
import json
import logging
import time
import urllib.parse

import requests
from bs4 import BeautifulSoup, Comment
from readability import Document

logger = logging.getLogger(__name__)


class ContentFetcher:
    """网页正文抓取器"""

    def __init__(self, config: dict):
        crawler_cfg = config.get("crawler", {})
        self.timeout = crawler_cfg.get("timeout", 15)
        self.delay = crawler_cfg.get("delay", 1)
        self.user_agent = crawler_cfg.get(
            "user_agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        self.max_retries = crawler_cfg.get("max_retries", 3)
        self.ad_keywords = crawler_cfg.get("ad_keywords", [
            "ad-", "advertisement", "adsbygoogle", "banner-ad",
            "sidebar-ad", "promo", "sponsored", "guanggao", "gg_",
            "tonglan", "duilian",
        ])

    def fetch_content(self, url: str) -> str:
        """抓取单个 URL 的正文内容（纯文本，已去广告）"""
        html = self._download(url)
        if not html:
            return ""

        # 先尝试标准提取
        text = self._extract_text(html)
        if text and len(text) > 50:
            return text.strip()

        # 如果标准提取失败，尝试从 HTML 源码中直接提取正文
        text = self._fallback_extract(html, url)
        if text and len(text) > 50:
            return text.strip()

        return ""

    def _download(self, url: str) -> str:
        """下载网页 HTML，支持重试"""
        # 对 URL 中的中文字符进行编码
        parsed = urllib.parse.urlsplit(url)
        encoded_path = urllib.parse.quote(parsed.path, safe='/:@!$&\'()*+,;=')
        encoded_query = urllib.parse.quote(parsed.query, safe='=&?/:@!$\'()*+,;')
        url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, encoded_path, encoded_query, parsed.fragment))

        headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": url,
        }
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=self.timeout, verify=False, allow_redirects=True)
                resp.encoding = resp.apparent_encoding or "utf-8"
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"[重试 {attempt}/{self.max_retries}] {url} 返回状态码 {resp.status_code}")
            except requests.RequestException as e:
                logger.warning(f"[重试 {attempt}/{self.max_retries}] {url} 请求失败: {e}")
            if attempt < self.max_retries:
                time.sleep(self.delay * attempt)
        logger.error(f"下载失败（已重试 {self.max_retries} 次）: {url}")
        return ""

    def _extract_text(self, html: str) -> str:
        """使用 readability 提取正文，再用 BeautifulSoup 清洗广告"""
        doc = Document(html)
        summary_html = doc.summary()

        soup = BeautifulSoup(summary_html, "lxml")

        # 移除广告元素
        self._remove_ads(soup)

        # 移除脚本、样式、注释
        for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
            tag.decompose()
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()

        # 提取纯文本
        text = soup.get_text(separator="\n")

        # 清理多余空行
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines)

    def _fallback_extract(self, html: str, url: str) -> str:
        """
        备用提取方案：当 readability 提取失败时，
        尝试从 HTML 源码中用正则/BeautifulSoup 提取正文内容
        """
        soup = BeautifulSoup(html, "lxml")

        # 移除脚本、样式等
        for tag in soup(["script", "style", "noscript", "iframe", "svg", "header", "nav"]):
            tag.decompose()

        # 移除广告
        self._remove_ads(soup)

        # 策略1：查找常见的正文容器
        content_selectors = [
            "article", ".article-content", ".article_body", ".content",
            ".rich_media_content", "#article-container", ".text",
            ".news-content", ".post-content", ".entry-content",
            "[class*='article']", "[class*='content-body']",
            "[class*='main-content']", "[class*='news-body']",
        ]
        for selector in content_selectors:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if len(text) > 100:
                    lines = [l for l in text.splitlines() if l.strip()]
                    return "\n".join(lines)

        # 策略2：查找最大的文本块（通常是正文）
        # 找所有 <p> 标签，合并文本
        paragraphs = soup.find_all("p")
        if paragraphs:
            texts = []
            for p in paragraphs:
                t = p.get_text(strip=True)
                if len(t) > 20:  # 过滤掉太短的段落
                    texts.append(t)
            if len(texts) >= 2:
                return "\n".join(texts)

        # 策略3：获取 body 中最长的 div 文本
        body = soup.find("body")
        if body:
            text = body.get_text(separator="\n", strip=True)
            lines = [l for l in text.splitlines() if len(l.strip()) > 15]
            if len(lines) >= 2:
                return "\n".join(lines)

        return ""

    def _remove_ads(self, soup: BeautifulSoup):
        """根据广告关键词移除广告元素"""
        # 通过 id/class 属性匹配
        for keyword in self.ad_keywords:
            for selector in [f"[id*='{keyword}']", f"[class*='{keyword}']"]:
                try:
                    for el in soup.select(selector):
                        el.decompose()
                except Exception:
                    pass

        # 移除常见的广告/推广标签
        ad_tags = soup.find_all(["aside", "footer"])
        for tag in ad_tags:
            tag_str = tag.get("id", "") + " " + " ".join(tag.get("class", []))
            if any(kw in tag_str.lower() for kw in self.ad_keywords):
                tag.decompose()

        # 移除包含"广告""推广"等文字的 div
        for div in soup.find_all("div"):
            text = div.get_text(strip=True)
            if not text:
                continue
            ad_text_patterns = ["广告", "推广", "赞助", "AD", "Sponsored", "Promoted"]
            if len(text) < 20 and any(p in text for p in ad_text_patterns):
                div.decompose()

        # 移除固定定位的浮层（通常是广告）
        for el in soup.find_all(style=True):
            style = el["style"].lower()
            if "position: fixed" in style or "position:fixed" in style:
                el.decompose()
