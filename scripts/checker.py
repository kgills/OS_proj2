#!/usr/bin/python

import sys, subprocess

print 'Opening output file:', str(sys.argv[1])
output_file = open(sys.argv[1], 'r')

n_i = "-1"
for line in output_file:
	line = line.split(" ")

	if(line[0] == "Enter"):
		if(n_i != "-1"):
			print("Colision!")
			output_file.close()
			exit(0)
		n_i = line[2][0]
	elif(line[0] == "Exit"):
		if(n_i != line[2][0]):
			print("Colision!")
			output_file.close()
			exit(0)
		else:
			n_i="-1"

print("Done Checking!")
