
dataprocess将对维基开源数据wikidata, wikipedia进行预处理。
## wikipedia  
### data
`data`存放原始数据和处理后的数据。

### wiki extractor
首先使用Wiki extractor对wikidump数据进行抽取。wikiextractor位于`wikipedia`文件夹下。

有关Wiki extractor的详情请参考链接[Wiki extractor](https://github.com/attardi/wikiextractor)
的简单用法。  
我们修改了该抽取器中`extract.sh`文件，将TEMPLATES参数注释掉，然后设置bytes参数为100M：
```shell
INPUT=$1
PROCESSES=$2
# TEMPLATES=$3
OUTPUT=$3

python WikiExtractor.py $INPUT \
       --json \
       --processes $PROCESSES \
       --output $OUTPUT \
       --bytes 100M \
       --json \
       --links \
       --sections \
       --lists \
       --keep_tables \
       --min_text_length 0 \
       --filter_disambig_pages

```
运行命令如下：
```shell
cd wikiextractor
extract.sh ../data/wiki_dump_data/zhwiki-latest-pages-articles.xml.bz2 8 ../data/
pure_article_data/zhwikidata
extract.sh ../data/wiki_dump_data/enwiki-latest-pages-articles.xml.bz2 8 ../data/
pure_article_data/enwikidata
```

### get mentions
对数据进行下一步预处理，将中文维基中的繁体字转换为简体字，然后对每个实体页面中出现的其他实体进行统计。最终的文件存储至`data/pure_article_data`下的`analyzed_zhwikidata和analyzed_enwikidata`中。
```python
python getMentions.py --input_file ../data/pure_article_data/zhwikidata --output_file  ../data/pure_article_data/analyzed_zhwikidata
python getMentions.py --input_file ../data/pure_article_data/enwikidata --output_file ../data/pure_article_data/analyzed_enwikidata
```

## wikidata
### filter zhwiki and enwiki
wikidata的实体文件，压缩文件约36G，解压后约735G，[JSON链接](https://dumps.wikimedia.org/wikidatawiki/entities/)，[数据格式](https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON)。
对下载后的文件进行处理，保留包含zhwiki或enwiki的实体，保存到data/output文件夹中。
```python
python filterEnAndZhData.py --input ../data/latest-all.json --output ../data/output
```
### get vertex and edge json file
将上一步的输出文件，处理成给定格式的文件。
```python
python genVertexAndEdge.py --input ../data/output --output ../data/veoutput
```

## relation
### get property and relation
根据wiki开放的[API](https://www.wikidata.org/w/api.php)根据数据文件中出现过的属性/关系的id获取其详细信息。
```python
python getPropertyAndRelation.py --input ../data/veoutput --output ../data/result
```
`result`文件中每一行为一个属性/关系的JSON数据，包括id, en-label, zh-label, en-description, zh-description, en-aliases, zh-aliases，除了id其余为optional.