#!/usr/bin/python

import sys, subprocess

print 'Opening config file:', str(sys.argv[1])
config_file = open(sys.argv[1], 'r')

n = -1
d = -1
c = -1
iters = -1
n_m = 0
# n_p = 0
machines = []
# paths = []


# get each line
for line in config_file:
	
	# Split the line by white space
	line_split = line.split()

	# print line_split

	# Remove all of the stuff after the #
	if '#' in line_split:
		line_split = line_split[:line_split.index("#")]

	for word in line_split:
		if '#' in word:
			break

		# First number will be n
		if n == -1:
			n = int(word)
			print "n =",n
			continue

		if d == -1:
			d = int(word)
			print "d = ",d
			continue

		if c == -1:
			c = int(word)
			print "c = ",c
			continue

		if iters == -1:
			iters = int(word)
			print "iters = ",iters
			continue

		# Get the machines
		if ((n_m < n) and (n != -1)):
			n_m = n_m+1
			machines.append(line_split)
			break

		# # Get the paths
		# if ((n_p < n) and (n != -1)):
		# 	n_p = n_p+1
		# 	paths.append(line_split)
		# 	break


# Remove all of the extra characters
# i = 0
# j = 0
# for path in paths:
# 	j = 0
# 	for word in path:
# 		word = word.replace(")", "")
# 		word = word.replace("(", "")
# 		word = word.replace(",", "")
# 		paths[i][j] = word
# 		j=j+1
# 	i=i+1

# Build and execute the commands
# print machines
i = 0

for machine in machines:

	command = ["~/Workspace/advos_ass1/server/server",machines[i][2],machines[i][0]+"_output.txt",str(n),machines[i][0]]

	for machine2 in machines:
		command = command + machine2[1:]

	# command = command +[str(len(paths[i]) - 1)]+paths[i][1:]

	command = ["ssh","-o","StrictHostKeyChecking=no","khg140030@"+machine[1]]+command
	# command = " ".join(command)
	print command
	# p=subprocess.Popen(command)
	i = i+1

# p.wait()
