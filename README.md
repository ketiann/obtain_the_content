# obtain_the_content
根据URL抓取网页正文内容，剔除广告，写回数据表。

## 功能
- 读取数据源（Excel/MySQL）中 `flag=0` 的记录
- 抓取对应 `url` 的网页正文内容
- **双引擎抓取**：requests 快速抓取 + Playwright 浏览器渲染（自动降级）
- 自动剔除广告元素
- 将正文写入 `content_all` 字段，`flag` 改为 `1`
- 输出到 Excel 或 MySQL（通过配置文件切换）

## 项目结构
```
obtain_the_content/
├── config.yaml          # 配置文件（输入/输出分离）
├── main.py              # 主入口
├── fetcher.py           # 网页抓取与正文提取模块（requests + Playwright 双引擎）
├── data_handler.py      # 数据读写模块（Excel/MySQL）
├── requirements.txt     # 依赖
└── README.md
```

## 使用方法
1. 安装依赖：
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   playwright install-deps chromium
   ```
2. 编辑 `config.yaml`，配置输入文件路径和输出路径
3. 运行：`python main.py`

## 配置说明
- `input.type`: 数据源类型，`excel` 或 `mysql`
- `output.type`: 输出类型，`excel` 或 `mysql`
- `crawler.browser_timeout`: Playwright 浏览器超时时间（秒）
- `crawler.browser_domains`: 自定义需要浏览器渲染的域名列表
- 切换为 MySQL 时，填写对应的连接信息和 SQL 即可

## 抓取策略
1. 对已知的JS渲染站点（百家号、微博、百度百科等），直接使用 Playwright 浏览器渲染
2. 其他站点优先使用 requests 快速抓取
3. 若 requests 提取正文失败，自动降级到 Playwright
4. 正文提取使用 readability + BeautifulSoup，自动剔除广告
