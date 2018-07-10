import csv
import re
import os


txt_file=open('m7.txt', 'r', encoding='UTF-8');
csv_file_result1 = open('node.csv', 'w');
csv_file_result1.write('ID,Name,Label,Weight\n');

all_lines = txt_file.readlines();
for i in range(0,len(all_lines)):
	#print (all_lines[i].split('\t'));
	line=all_lines[i].split('\t');
	weight=all_lines[i].count(':');
	csv_file_result1.write(line[1]+','+line[1]+','+line[0]+','+str(weight)+'\n');
csv_file_result1.close();
csv_file_result2 = open('link.csv', 'w');
csv_file_result2.write('Source,Target\n');
for i in range(0,len(all_lines)):
	#print (all_lines[i].split('\t'));
	line=all_lines[i].split('\t');
	csv_file_result2.write(line[1]+',');
	index1=line[2].find('[');
	index2=line[2].find(',');
	csv_file_result2.write(line[2][index1+1:index2]);
	line[2]=line[2][index2+1:];
	#print(line[2]);
	csv_file_result2.write('\n');
	while line[2].find('|')!=-1:
		index1=line[2].find('|');
		index2=line[2].find(',');
		csv_file_result2.write(line[1]+',');
		csv_file_result2.write(line[2][index1+1:index2]);
		line[2]=line[2][index2+1:];
		#print(line[2]);
		csv_file_result2.write('\n');
csv_file_result2.close();
