CLASSES:=CH.class 

.PHONY: all clean

%.class: %.java
	javac $<


all: $(CLASSES)


clean:
	-rm *.class

