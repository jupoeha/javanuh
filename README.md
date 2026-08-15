<div align="center">

# Java-ws
这是一个基于 Java 实现的多协议代理服务器，支持 VLESS、Trojan 和 Shadowsocks 协议 over WebSocket，集成哪吒探针服务(v0或v1)，无需三方内核，资源占用更少。

---

Telegram交流反馈群组：https://t.me/eooceu

</div>>

## 功能特性
- ✅ 多协议支持：VLESS、Trojan、Shadowsocks over WebSocket
- ✅ 协议自动检测：自动识别客户端使用的协议
- ✅ 订阅生成：自动生成 Base64 格式的订阅链接
- ✅ 哪吒监控集成：支持哪吒监控 v0 和 v1 版本
- ✅ 环境变量配置：支持系统环境变量和 .env 文件
- ✅ 域名屏蔽：自动屏蔽测速网站
- ✅ DNS 缓存：减少 DNS 查询，提高性能
- ✅ 静默模式：非 DEBUG 模式下只显示关键日志
- ✅ 自动端口检测：端口被占用时自动寻找可用端口

* PaaS 平台设置的环境变量
  | 变量名        | 是否必须 | 默认值 | 备注 |
  | ------------ | ------ | ------ | ------ |
  | UUID         | 否 |7bd180e8-1142-4387-93f5-03e8d750a896| 开启了哪吒v1,请修改UUID|
  | PORT         | 否 |  3000  |  节点监听端口,默认自动获取分配的端口                  |
  | NEZHA_SERVER | 否 |        |哪吒v1填写形式：nz.abc.com:8008   哪吒v0填写形式：nz.abc.com|
  | NEZHA_PORT   | 否 |        | 哪吒v1没有此变量，v0的agent端口| 
  | NEZHA_KEY    | 否 |        | 哪吒v1的NZ_CLIENT_SECRET或v0的agent端口 |
  | NAME         | 否 |        | 节点名称前缀，例如：koyeb |
  | DOMAIN       | 是 |        | 项目分配的域名或已反代的域名，不包括https://前缀  |
  | SUB_PATH     | 否 |  sub   | 订阅token    |
  | AUTO_ACCESS  | 否 |  false | 是否开启自动访问保活,false为关闭,true为开启,需同时填写DOMAIN变量 |
  | DEBUG        | 否 |  false | 调试模式，默认关闭，true开启                   |

* 域名/${SUB_APTH}查看节点信息，非标端口，域名:端口/${SUB_APTH}  SUB_APTH为自行设置的订阅token，未设置默认为sub

* 自行在[Release](https://github.com/eooce/java-ws/releases/tag/latest)中下载jar文件上传到java服务器运行

### 使用cloudflare workers 或 snippets 反代域名给节点套cdn加速,也可以使用端口回源方式
```
export default {
    async fetch(request, env) {
        let url = new URL(request.url);
        if (url.pathname.startsWith('/')) {
            var arrStr = [
                'change.your.domain', // 此处单引号里填写你的节点伪装域名
            ];
            url.protocol = 'https:'
            url.hostname = getRandomArray(arrStr)
            let new_request = new Request(url, request);
            return fetch(new_request);
        }
        return env.ASSETS.fetch(request);
    },
};
function getRandomArray(array) {
  const randomIndex = Math.floor(Math.random() * array.length);
  return array[randomIndex];
}
```

版权所有 ©2026 `eooce`



