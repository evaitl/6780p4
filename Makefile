CLASSES:=BNS.class NS.class 

.PHONY: all clean indent

%.class: %.java
	javac $<


all: $(CLASSES)


clean:
	-rm *.class


indent:
	uncrustify --no-backup -c docs/uncrustify.cfg *.java
