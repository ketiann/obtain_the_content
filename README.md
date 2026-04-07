# obtain_the_content
根据URL抓取网页正文内容，剔除广告，写回数据表。

## 功能
- 读取数据源（Excel/MySQL）中 `flag=0` 的记录
- 抓取对应 `url` 的网页正文内容
- 自动剔除广告元素
- 将正文写入 `content_all` 字段，`flag` 改为 `1`
- 输出到 Excel 或 MySQL（通过配置文件切换）

## 项目结构
```
obtain_the_content/
├── config.yaml          # 配置文件（输入/输出分离）
├── main.py              # 主入口
├── fetcher.py           # 网页抓取与正文提取模块
├── data_handler.py      # 数据读写模块（Excel/MySQL）
├── requirements.txt     # 依赖
└── README.md
```

## 使用方法
1. 安装依赖：`pip install -r requirements.txt`
2. 编辑 `config.yaml`，配置输入文件路径和输出路径
3. 运行：`python main.py`

## 配置说明
- `input.type`: 数据源类型，`excel` 或 `mysql`
- `output.type`: 输出类型，`excel` 或 `mysql`
- 切换为 MySQL 时，填写对应的连接信息和 SQL 即可
