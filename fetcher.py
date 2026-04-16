"""
网页抓取与正文提取模块
策略：
  1. 优先使用 requests 快速抓取 + readability 提取
  2. 若提取失败，自动降级到 Playwright 无头浏览器渲染后再提取
  3. 自动剔除广告元素
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

# 需要使用浏览器渲染的域名列表
BROWSER_REQUIRED_DOMAINS = [
    "baijiahao.baidu.com",
    "m.baidu.com",
    "weibo.com",
    "weibo.cn",
    "baike.baidu.com",
    "www.toutiao.com",
    "www.douyin.com",
    "mp.weixin.qq.com",
]


class ContentFetcher:
    """网页正文抓取器（支持 requests + Playwright 双引擎）"""

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
        self.browser_timeout = crawler_cfg.get("browser_timeout", 20)
        self._browser = None

    def _get_browser(self):
        """懒加载 Playwright 浏览器实例"""
        if self._browser is None:
            try:
                from playwright.sync_api import sync_playwright
                self._pw = sync_playwright().start()
                self._browser = self._pw.chromium.launch(headless=True)
                logger.info("Playwright 浏览器引擎已启动")
            except Exception as e:
                logger.error(f"Playwright 启动失败: {e}")
                self._browser = False  # 标记为不可用
        return self._browser if self._browser else None

    def close(self):
        """关闭浏览器"""
        if self._browser:
            try:
                self._browser.close()
                self._pw.stop()
            except Exception:
                pass
            self._browser = None

    def _needs_browser(self, url: str) -> bool:
        """判断该 URL 是否需要浏览器渲染"""
        from urllib.parse import urlparse
        domain = urlparse(url).hostname or ""
        return any(domain == d or domain.endswith("." + d) for d in BROWSER_REQUIRED_DOMAINS)

    def fetch_content(self, url: str) -> str:
        """抓取单个 URL 的正文内容（纯文本，已去广告）"""
        try:
            return self._fetch_content_impl(url)
        except Exception as e:
            logger.error(f"抓取异常: {url} - {e}")
            return ""

    def _fetch_content_impl(self, url: str) -> str:
        use_browser = self._needs_browser(url)

        if use_browser:
            # 对需要浏览器的域名，直接使用浏览器
            html = self._download_with_browser(url)
            if html:
                text = self._extract_text(html)
                if text and len(text) > 50:
                    return text.strip()
                text = self._fallback_extract(html, url)
                if text and len(text) > 50:
                    return text.strip()
            # 浏览器也失败，尝试 requests 兜底
            logger.info(f"  浏览器抓取失败，尝试 requests 兜底: {url}")

        # 标准 requests 抓取
        html = self._download(url)
        if not html:
            # requests 也失败，如果之前没用过浏览器，再尝试一次
            if not use_browser:
                html = self._download_with_browser(url)
            if not html:
                return ""

        text = self._extract_text(html)
        if text and len(text) > 50:
            return text.strip()

        text = self._fallback_extract(html, url)
        if text and len(text) > 50:
            return text.strip()

        return ""

    # ======================== requests 下载 ========================

    def _download(self, url: str) -> str:
        """使用 requests 下载网页 HTML"""
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
        logger.error(f"requests 下载失败（已重试 {self.max_retries} 次）: {url}")
        return ""

    # ======================== Playwright 浏览器下载 ========================

    def _download_with_browser(self, url: str) -> str:
        """使用 Playwright 无头浏览器下载渲染后的 HTML"""
        browser = self._get_browser()
        if not browser:
            return ""

        for attempt in range(1, self.max_retries + 1):
            try:
                context = browser.new_context(
                    user_agent=self.user_agent,
                    viewport={"width": 1920, "height": 1080},
                    locale="zh-CN",
                )
                page = context.new_page()

                # 拦截并屏蔽广告/追踪请求，加速加载
                page.route("**/*", self._block_ads_route)

                page.goto(url, wait_until="domcontentloaded", timeout=self.browser_timeout * 1000)

                # 等待正文内容出现（针对百家号等 SPA 页面）
                try:
                    page.wait_for_selector("article, .article-content, .content, [class*='article'], #article-container",
                                           timeout=8000)
                except Exception:
                    pass

                # 额外等待一小段时间让JS渲染完成
                time.sleep(1.5)

                html = page.content()
                context.close()
                return html

            except Exception as e:
                logger.warning(f"[浏览器重试 {attempt}/{self.max_retries}] {url} 失败: {e}")
                try:
                    context.close()
                except Exception:
                    pass
                if attempt < self.max_retries:
                    time.sleep(self.delay * attempt)

        logger.error(f"浏览器下载失败（已重试 {self.max_retries} 次）: {url}")
        return ""

    def _block_ads_route(self, route):
        """拦截广告和追踪请求"""
        url = route.request.url
        blocked = [
            "doubleclick.net", "googlesyndication.com", "googleadservices.com",
            "baidustatic.com/shield/", "pos.baidu.com", "cpro.baidu.com",
            "analytics", "tracker", "beacon", "hm.baidu.com",
            "cnzz.com", "scorecardresearch.com",
        ]
        resource_types = ["image", "media", "font", "websocket"]
        if any(b in url for b in blocked) or route.request.resource_type in resource_types:
            route.abort()
        else:
            route.continue_()

    # ======================== 正文提取 ========================

    def _extract_text(self, html: str) -> str:
        """使用 readability 提取正文，再用 BeautifulSoup 清洗广告"""
        try:
            doc = Document(html)
            summary_html = doc.summary()
        except Exception:
            return ""
        try:
            soup = BeautifulSoup(summary_html, "lxml")
        except Exception:
            return ""
        self._remove_ads(soup)

        for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
            tag.decompose()
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()

        text = soup.get_text(separator="\n")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines)

    def _fallback_extract(self, html: str, url: str) -> str:
        """备用提取方案：从 HTML 源码中用 BeautifulSoup 提取正文"""
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            try:
                soup = BeautifulSoup(html, "html.parser")
            except Exception:
                return ""

        for tag in soup(["script", "style", "noscript", "iframe", "svg", "header", "nav"]):
            tag.decompose()
        self._remove_ads(soup)

        # 策略1：查找常见的正文容器
        content_selectors = [
            "article", ".article-content", ".article_body", ".content",
            ".rich_media_content", "#article-container", ".text",
            ".news-content", ".post-content", ".entry-content",
            "[class*='article']", "[class*='content-body']",
            "[class*='main-content']", "[class*='news-body']",
            # 百家号特有选择器
            "[class*='index-module']", ".index-wrap",
        ]
        for selector in content_selectors:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if len(text) > 100:
                    lines = [l for l in text.splitlines() if l.strip()]
                    return "\n".join(lines)

        # 策略2：找所有 <p> 标签
        paragraphs = soup.find_all("p")
        if paragraphs:
            texts = []
            for p in paragraphs:
                t = p.get_text(strip=True)
                if len(t) > 20:
                    texts.append(t)
            if len(texts) >= 2:
                return "\n".join(texts)

        # 策略3：body 文本
        body = soup.find("body")
        if body:
            text = body.get_text(separator="\n", strip=True)
            lines = [l for l in text.splitlines() if len(l.strip()) > 15]
            if len(lines) >= 2:
                return "\n".join(lines)

        return ""

    def _remove_ads(self, soup: BeautifulSoup):
        """根据广告关键词移除广告元素"""
        for keyword in self.ad_keywords:
            for selector in [f"[id*='{keyword}']", f"[class*='{keyword}']"]:
                try:
                    for el in soup.select(selector):
                        el.decompose()
                except Exception:
                    pass

        ad_tags = soup.find_all(["aside", "footer"])
        for tag in ad_tags:
            tag_str = tag.get("id", "") + " " + " ".join(tag.get("class", []))
            if any(kw in tag_str.lower() for kw in self.ad_keywords):
                tag.decompose()

        for div in soup.find_all("div"):
            text = div.get_text(strip=True)
            if not text:
                continue
            ad_text_patterns = ["广告", "推广", "赞助", "AD", "Sponsored", "Promoted"]
            if len(text) < 20 and any(p in text for p in ad_text_patterns):
                div.decompose()

        for el in list(soup.find_all(True)):
            try:
                style = (el.get("style") or "") if el.attrs else ""
                if style and ("position: fixed" in style.lower() or "position:fixed" in style.lower()):
                    el.decompose()
            except (AttributeError, TypeError):
                pass
