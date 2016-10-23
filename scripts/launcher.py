#!/usr/bin/python

import sys, subprocess

print 'Opening config file:', str(sys.argv[1])
config_file = open(sys.argv[1], 'r')

# Edit this to the path where the executables are
path="~/Workspace/AdvancedOS/OS_proj2/"

# Edit this for your username
user="khg140030@"

n = -1
d = -1
c = -1
iters = -1
n_m = 0
n_q = 0
machines = []
quorums = []

# get each line
for line in config_file:
	
	# Split the line by white space
	line_split = line.split()

	# Remove all of the stuff after the #
	if '#' in line_split:
		line_split = line_split[:line_split.index("#")]

	for word in line_split:
		if '#' in word:
			break

		# First number will be n
		if n == -1:
			n = int(word)
			continue

		if d == -1:
			d = int(word)
			continue

		if c == -1:
			c = int(word)
			continue

		if iters == -1:
			iters = int(word)
			continue

		# Get the machines
		if ((n_m < n) and (n != -1)):
			n_m = n_m+1
			machines.append(line_split)
			break

		# Get the quorums
		if ((n_q < n) and (n != -1)):
			n_q = n_q+1
			quorums.append(line_split)
			break

# Remove all of the extra characters
i = 0
j = 0
for quorum in quorums:
	j = 0
	for word in quorum:
		word = word.replace(")", "")
		word = word.replace("(", "")
		word = word.replace(",", "")
		quorums[i][j] = word
		j=j+1
	i=i+1

# Build and execute the commands
# print machines
i = 0
for machine in machines:

	command = [path+"java Maekawa",str(n),machines[i][0], str(d), str(c), str(iters)]

	for machine2 in machines:
		command = command + machine2[1:]

	command = command +[str(len(quorums[i]) - 1)]+quorums[i][1:]

	command = ["ssh","-o","StrictHostKeyChecking=no",user+"@"+machine[1]]+command
	print " ".join(command)
	i = i+1

	# p=subprocess.Popen(command)

# p.wait()
