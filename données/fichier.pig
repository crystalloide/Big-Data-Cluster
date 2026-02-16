A = LOAD '/data/input/montexte.txt' USING TextLoader AS ligne:chararray;
DUMP A;
DESCRIBE A;

B = FOREACH A GENERATE TOKENIZE(LOWER(ligne)) AS mots;
DUMP B; 
DESCRIBE B;

C = FOREACH B GENERATE FLATTEN(mots) AS mot;
DUMP C;
DESCRIBE C;

D = GROUP C BY mot;
DUMP D; 
DESCRIBE D;

E = FOREACH D GENERATE group AS mot, COUNT(C) as occurrences;
DUMP E; 
DESCRIBE E;

STORE E INTO '/data/output/wcpig';