
utils模块的作用是对wikiextractor的结果进行处理：全部替换为简体字，并获取每篇text中的mention及其链接。  

## wiki extractor
首先，我们使用Wiki extractor对wikidump数据进行抽取。wikiextractor位于`utils`文件夹下。

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

## Get mentions
我们在运行get_mentions.py脚本前，使用Wiki extractor对Wikidump数据进行了处理。
运行命令如下：
```shell
cd wikiextractor
extract.sh ../../data/wiki_dump_data/zhwiki-latest-pages-articles.xml.bz2 8 ../../data/
pure_article_data/zhwikidata
extract.sh ../../data/wiki_dump_data/enwiki-latest-pages-articles.xml.bz2 8 ../../data/
pure_article_data/enwikidata
```

在使用extractor进行抽取后，文件应该放置在`data/pure_article_data`文件夹下`的zhwikidata`和`enwikidata`中。
我们对数据进行下一步预处理，将中文维基中的繁体字转换为简体字，然后对每个实体页面中出现的其他实体进行统计。最终的文件存储至`data/pure_article_data`下的`analyzed_zhwikidata和analyzed_enwikidata`中。
```python
python get_mentions.py --input_file ../data/pure_article_data/zhwikidata --output_file  ../data/pure_article_data/analyzed_zhwikidata
python get_mentions.py --input_file ../data/pure_article_data/enwikidata --output_file ../data/pure_article_data/analyzed_enwikidata
```

## Filter zhwiki and enwiki
wikidata的实体文件，压缩文件约36G，解压后约735G，[JSON链接](https://dumps.wikimedia.org/wikidatawiki/entities/)，[数据格式](https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON)。
对下载后的文件进行处理，保留包含zhwiki或enwiki的实体，保存到output文件夹中。
```
python filter_en_zh.py --input latest-all.json --output output
```