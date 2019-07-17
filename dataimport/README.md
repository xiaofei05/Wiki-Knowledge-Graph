Maven项目，对JanusGraph数据库建模与使用。
## 使用
1. 打包
```
mvn clean package
```
2. 使用生成的jar中建模
```
java -cp target/dataimport-1.0-SNAPSHOT.jar com.schema.SchemaMain <janusgraph-config-file(.properties)> schema.json
```
3. 导入顶点
```
java -cp target/dataimport-1.0-SNAPSHOT.jar com.loaddata.LoadDataMain <janusgraph-config-file(.properties)> </veoutput/entity/> vertex <thread num>
```
4. 导入边
```
java -cp target/dataimport-1.0-SNAPSHOT.jar com.loaddata.LoadDataMain <janusgraph-config-file(.properties)> </veoutput/relation/> edge <thread num>
```