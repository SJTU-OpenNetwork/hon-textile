# Textile2 开发进度
## 总体目标
使用textile团队维护的新的 [go-threads](https://github.com/textileio/go-threads) 库作为现有Thread结构的替代方案。
所有目前由Thread完成的功能，包括消息发送和同步、群组维护都通过go-threads实现。

## 进度
- [ ] go-threads基本运行
    - [ ] 使用原有host和repo目录创建go-threads服务
    - [ ] 实现byte数组往thread上的添加。
    - [ ] 实现cmd对thread的创建、查看、添加、更新监听、历史获取
    - [ ] 对安卓端开放thread更新的监听接口，确保可以在移动端稳定运行
- [ ] 使用go-threads实现消息发送
    - [ ] 定义结构化的msg，以及其序列化、反序列化方式。最方便的方式是通过pb实现。
- [ ] 使用go-threads实现群组管理
    - [ ] 单个thread代表群组，实现群组的添加接口。
    - [ ] 为thread添加访问权限。
    - [ ] 群组成员管理能力，能够感知群组成员变动。但是发送信息不再依赖于群组成员列表，成员列表仅仅用于显示。
    

## go-threads架构
### 工作方式
thread本身是一个分布式数据库，多个节点可以通过<b>添加</b>或<b>创建</b>
两个动作来获取一个thread。thread的更新会在其拥有者之间同步，拥有者可以
向thread添加内容、监听thread更新、或者主动拉取thread历史。
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