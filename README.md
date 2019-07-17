# 知识图谱项目
本项目旨在利用维基数据，创建一个通用的知识图谱平台。  

## 数据预处理 
`dataprocess`对wiki-pedia和wiki-data开源数据进行预处理。
* [Wikidump](https://dumps.wikimedia.org/)
* [Wikidata](https://dumps.wikimedia.org/wikidatawiki/entities/)

## 导入数据到JanusGraph
`dataimport`定义JanusGraph的schema，并导入处理后的数据。



# references

**JanusGraph**: https://github.com/JanusGraph/janusgraph

**JanusGraph-utils**: https://github.com/IBM/janusgraph-utils