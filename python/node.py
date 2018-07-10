import codecs

file_name = 'm7.txt'
encode = 'UTF-8'
node_csv = 'node.csv'
edge_csv = 'edge.csv'

with codecs.open(file_name, encoding=encode) as file:
  labels = {}
  maps = {}
  with codecs.open(edge_csv, mode='w', encoding=encode) as edge_file:
    edge_file.write('source,target\n')
    for line in file.readlines():
      split = line.split("\t")
      label = split[0]
      name = split[1]
      labels[name] = label
      value = split[2][1:-2]
      v_split = value.split("|")
      for v in v_split:
        _v_split_left = v.split(",")
        _v_split_right = _v_split_left[1].split(":")
        v_name = _v_split_left[0]
        v_label = _v_split_right[0]
        v_weight = float(_v_split_right[1])
        if v_name not in maps:
          maps[v_name] = 0
        maps[v_name] += v_weight
        edge_file.write(
          '{source},{target}\n'.format(
            source=name,
            target=v_name
          )
        )
  with codecs.open(node_csv, mode='w', encoding=encode) as node_file:
    node_file.write('ID,label,name,weight\n')
    for name in maps:
      node_file.write(
        '{ID},{label},{name},{weight}\n'.format(
          ID=name,
          label=name,
          name=labels[name],
          weight=maps[name]
        )
      )
