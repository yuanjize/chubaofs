## 版本 v1.0.0 - 2020/03/15

ChubaoFS挂载客户端增加两个新特性，修复一个已知问题。[下载]( http://storage.jd.local/dpgimage/cfsdbbak/cfs-client-withToken)。

### 新特性
* 引入读写token控制，增强数据安全性。

ChubaoFS申请及旧挂载方式可参考[文档](http://tigcms.jd.com/details/H1VX9KHfN.html)

每个Volume的`只读Token`和`读写Token`可自行查询[jfs.jd.com](http://jfs.jd.com/)

将查询到的`只读Token`或`读写Token`替换掉配置文件中的`token`后，即可在挂载ChubaoFS客户端后进入只读或读写模式。

```yaml
{
  "mountpoint": "/cfs/mnt",
  "volname": "ltptest",
  "master": "10.196.31.173:80,10.196.31.141:80,10.196.30.200:80",
  "logpath": "/cfs/log",
  "profport": "10094",
  "loglvl": "debug",
  "token": "bHRwdGVzdCMxIzE1NzY3MjcxNzc="
}
```
请注意：由于`rdonly`属性在新版本已被更为安全的`token`机制取代，请将原配置文件里的`rdonly`属性删除。

* 支持自定义extent大小。 http://git.jd.com/chubaofs/chubaofs/commit/c0444312d6401497bb7d423873b6e352c846524a

### 修复
* 网络错误导致产生垃圾dentry。
