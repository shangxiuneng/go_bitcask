## 项目说明：
基于bitcask模型，实现的高性能kv存储引擎
1. 采用golang语言开发，基于bitcask模型，实现了数据快速，高效，稳定的检索，具有高吞吐量和低读写放大的特性。
2. 实现了数据的持久化，确保数据的可靠性和可恢复性。
3. 支持哈希，b树，art树，b+树索引结构。
4. 支持数据的原子写能力。
5. 使用锁机制，保证了数据的一致性和并发访问的正确性

### bitcaks论文地址
https://riak.com/assets/bitcask-intro.pdf

### 部分实现说明

### 如何使用

### QA
#### 1. 和redis的区别？
##### 数据存储模型方面  
redis全部的kv均存储在内存中，适合对读写有比较高要求的场景。  
bitcask的数据存储在磁盘上，存储结构类似lsm tree，适合读少写多的场景。
##### 持久化方面
redis提供了rdb和aof两种日志持久化方式。redis虽然可以选择更加频繁的持久化方式，但是会增加性能开销。  
bitcask本身是通过追加磁盘的方式进行持久化。
#### 内存使用方面
redis是将kv全部存储在内存中，因此能够存储的kv上限受到内存限制。  
bitcask将kv数据存储在磁盘上，因此能够比redis存储更多的数据。
#### 使用场景
redis适合需要超低延迟和快速响应的场景，例如缓存、实时计数、排行榜、消息队列等。  
bitcask适合写密集的场景，比如日志存储。

#### 2. 和其他bitcask开源项目相比，你的项目有什么不同？
- 支持的索引模型更多。大多数bitcask模型的实现都是基于内存，本项目提供了基于磁盘的b+树。
#### 3. 本项目的优缺点
##### 优点：  
- 写入性能好  
- 存储数据的上限不受限于内存
##### 缺点：
- 所有的key都必须在内存中维护
- 数据库启动时，需要遍历所有的文件构建内存索引，当数据量比较多时，可能会导致启动时间过长（本项目通过mmap的方式在数据库启动时构建内存索引，加速了这一过程）。
#### 4. 为什么想要做该项目？
对数据库的实现比较感兴趣，因此有了该项目。
