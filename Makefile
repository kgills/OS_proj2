all: Maekawa.java
	javac -g $^

clean:
	rm -rf *.class
	rm -f Maekawa.txt
	
