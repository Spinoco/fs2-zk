# Apache Zookeeper bindings for fs2 

Simple library that allws you to consume Apache Zookeeper in program using Functional Streams for Scala (fs2). This wraps around the Appache Zookeeper Client and provides some basic functionality to help you work with Zookeeper easily. 

Library is WIP now, and will be published once stable release of fs2 0.9 release will be released. Until then you need to compile master of fs2 locally. 

Simple usage :

```scala 
import spinoco.fs2.zk.client

// monitor all children of given node 'node1' 
// by discrete stream of changes 
client("yourZkConnectString") flatMap { zkc =>  
   clientTo(zks) flatMap { zkc => zkc.childrenOf(node1) } 
}

```

More examples you may found in Tests [here](https://github.com/Spinoco/fs2-zk/blob/master/src/test/scala/spinoco/fs2/zk/ZkClientSpec.scala)

