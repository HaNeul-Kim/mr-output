#! /bin/bash
# proto file 로 fbs 파일 생성 가능하나 root_type
# flatc --proto ../proto/Yellow.proto
flatc --java Yellow.fbs
mkdir -p ../../java/com/tistory/hskimsky/mapreduce/flatbuf/
mv com/tistory/hskimsky/mapreduce/flatbuf/YellowFlat.java ../../java/com/tistory/hskimsky/mapreduce/flatbuf/
rm -rf com/
