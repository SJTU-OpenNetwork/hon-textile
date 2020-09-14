# Textile2 开发进度
## 总体目标
使用textile团队维护的新的 [go-threads](https://github.com/textileio/go-threads) 库作为现有Thread结构的替代方案。
所有目前由Thread完成的功能，包括消息发送和同步、群组维护都通过go-threads实现。

## 进度
- [ ] go-threads基本运行
    - [ ] 使用原有host和repo目录创建go-threads服务
    - [ ] 实现节点间消息的发送
    - [ ] 创建public thread用于测试
    - [ ] 实现cmd对public thread的查看和添加
    - [ ] 对安卓端开放public thread接口，确保可以在移动端稳定运行
    
## go-threads架构
### 接口定义
- core.app.Net <code>Interface</code>
  - core.net.Net
    - core.net.API
      - Thread整体操作接口，包括Thread的创建、添加、获取、删除
      - Record操作相关接口，包括Record的创建、向Thread添加、获取
      - Subscribe接口
    - go-ipld-format.DAGService
      - 包括各种直接对DAG进行操作的接口，例如CID对应数据获取，向DAG中添加删除Node等。
    - Host() go-libp2p-core/host.Host
  - ConnectApp(core.app.App, thread.ID) (*Connector, error)
  将APP与Thread绑定
  - Validate(...)
- core.app.App <code>Interface</code>
  - ValidateNetRecordBody() 提供应用定义的Record合法性检测接口。
  - HandleNetRecord() 对Thread新更新的Record进行处理的接口。

### 接口实现
<code>core.net.net Struct</code>实现了最核心的<code>core.app.Net</code>接口，
通过<code>core.net.NewNetwork(...)</code>创建。