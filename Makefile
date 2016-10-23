all: Maekawa.java
	javac -g $^

clean:
	rm -rf *.class
	
