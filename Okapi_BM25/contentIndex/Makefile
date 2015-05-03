sourcefiles = *.java

classfiles  = $(sourcefiles:.java=.class)

all: $(classfiles)

%.class: %.java
	javac *.java
clean:
	rm -f *.class

