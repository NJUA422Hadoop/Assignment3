# 解决MacOS下license大小写不敏感的问题。
zip -d target/wuxia.jar META-INF/LICENSE
jar tvf target/wuxia.jar | grep -i license