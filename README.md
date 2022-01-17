# arbitrage_coin
分批建仓

## 使用方法
安装依赖项：
```
pip3 install -r requirements.txt
```
调整参数：
```
调整batches_build.py
Build(symbol).__init__()下的参数
```
最后在终端中运行：
```
python3 batches_build.py
```

## 消息推送
- Linux/macOS：
  终端中运行 `crontab -e`，在文件末尾加上一行（注意换成实际的路径）
  ```
  * * * * * /path/to/python3 /path/to/arbitrage_coin/batches_build.py
  ```


## 参考说明
感谢[@hengxuZ](https://github.com/hengxuZ)
根据[spot-trend-grid](https://github.com/hengxuZ/spot-trend-grid)
修改如有侵权，请联系我删除

## 特别声明
* 本人对任何脚本问题概不负责，包括但不限于由任何脚本错误导致的任何损失或损害。
* 请勿将本仓库的任何内容用于商业或非法目的，否则后果自负。
* 任何以任何方式查看此项目的人或直接或间接使用该项目的任何脚本的使用者都应仔细阅读此声明。本人保留随时更改或补充此免责声明的权利。一旦使用并复制了任何相关脚本的规则，则视为您已接受此免责声明。

## 关于打赏
如果对你有帮助可以点个赞哦，或者打赏作者一杯咖啡哟～

另外通过我邀请码注册的用户，充值并交易返现5元。
<img width="609" alt="历史收益" src="https://user-images.githubusercontent.com/11848358/149764288-3c4241af-cc72-4633-bbb8-95f222bd0867.jpg">
<img width="609" alt="支付宝" src="https://user-images.githubusercontent.com/11848358/143569245-7b1fe1bd-d170-4327-b729-459ae191fbc1.png">
<img width="609" alt="微信" src="https://user-images.githubusercontent.com/11848358/143569385-553ec9d1-0cad-44bf-8dcb-c444218832c0.png">