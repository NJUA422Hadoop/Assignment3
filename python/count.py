import codecs

maps = {}

with codecs.open('test/m7.txt', encoding='utf-8') as file:
  for line in file.readlines():
    split = line.split('\t')
    label = split[0]
    if label in maps:
      maps[label] += 1
    else:
      maps[label] = 0

for label in maps:
  print(label, maps[label])