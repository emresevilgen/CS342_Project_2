
all: mvt_s

mvt_s: mvt_s.c
	gcc -Wall  -o mvt_s mvt_s.c -lpthread

clean: 
	rm -fr *~  mvt_s