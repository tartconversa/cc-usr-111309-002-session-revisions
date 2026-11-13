# 迟到事件桥接与拆分的会话事实层

本项目保存虚构实验设施的仓湖源数据与Python读取工具。现有工具只加载文件并显示条数，不执行事实加工、解码、合并、联接或重映射。全部设备、样品、记录和数值均为合成。

`src/session_builder.py` 在读取层之上提供**可修订会话事实构建器**：仅对合成设备事件做会话归并与版本修订，不做用户行为模式挖掘、画像或轨迹分析。`src/replay_revisions.py` 按到达顺序重放事件并产出事实账本、历史快照与修订日志。

## 字段与单位

event_at_seconds从共同起点计；revision在event_id内比较，withdrawn=true撤回该事件。gap_seconds是相邻事件连接阈值，不是会话首尾上限。

material-index.json记录全部输入内容哈希；fixtures保存原始资料与配置，计算输出可放.workspace-output，原始资料不应被覆盖。

## 会话与修订规则

- 裁决：同event_id取最高revision；最高revision平级且内容不一致为平级冲突，隔离不参与构建，直到更高revision解除。重送（重复或内容一致的更高revision）幂等，不推进版本。
- 每设备按(event_at_seconds, event_id)排序，相邻间隔 <= gap_seconds 即连接，会话是连接链的极大分量；链总跨度可以远超gap。session_key取成员event_id字典序最小值，与时间先后无关。单点会话时长0是合法测量，空设备不造会话。
- 增量修订：受影响范围沿相邻间隔扩展到断开边界（间隔 > gap），不使用固定时间窗，也不全表清空重算。闭包内旧会话事实与成员桥先撤销、再在同一版本原子发布新集合；闭包外事实fact_id保持不变。
- 每次写入进入仅追加账本（publish/revoke），任意历史版本可通过`snapshot(device, version)`查询；修订日志记录old->new血缘（created/removed/merged/split/key_changed/members_updated）、受影响成员与守恒计数，稳定键变化显式标注而不计为新增访问。
- 多设备各自独立版本号与锁，可并行构建且互不阻塞；同设备一次修订原子可见。每次事实变化都与全量参考重算交叉核对（局部索引 vs 全量参考，参考只核对不替代增量）。

## 运行已有读取器

Python 3.11以上，标准库即可，不需安装第三方依赖。

    python -m src.session_reader fixtures/events.jsonl
    python -m src.replay_revisions fixtures/events.jsonl fixtures/sessions.json --output-dir .workspace-output/demo
    python -m compileall -q src tests
    python -m unittest discover -s tests -v

读取不受信输入文件时用隔离模式并显式传入路径（-I不读PYTHONPATH/当前目录）：

    python -I -c "import sys; sys.path.insert(0, '.'); import unittest; \
        unittest.TextTestRunner().run(unittest.TestLoader().discover('tests'))"

## 固定容器

    docker build -t session-revisions:local .
    docker run --rm session-revisions:local

容器镜像以完整digest固定，默认仅检查输入契约；实现新增分析功能所需依赖由开发者明确登记。
