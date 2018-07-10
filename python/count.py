import codecs

maps = {}

with codecs.open('m7.txt', encoding='utf-8') as file:
  for line in file.readlines():
    split = line.split('\t')
    label = split[0]
    if label in maps:
      maps[label] += 1
    else:
      maps[label] = 1

for label in sorted(maps, key=lambda k: maps[k], reverse=True):
  print(label, maps[label])