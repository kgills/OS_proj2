all: maekawa.java
	javac -g $^

clean:
	rm -rf maekawa.class
	
