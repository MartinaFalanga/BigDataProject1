# Primo progetto di Big Data

Il Progetto può essere suddiviso in quattro fasi:
1) Pulizia del dataset,
2) Creazione del primo Job,
3) Creazione del secondo Job,
4) Creazione del terzo Job,

Obiettivo: progettare e realizzare in MapReduce, Hive, Spark i tre job.

## Pulizia del dataset
Durante la pulizia del dataset c'è stato bisogno di un'analisi del dataset stesso con annessa rimozione di colonne insignificanti le quali contenvano anche valori nulli ed eliminazione dei duplicati.

## Primo Job
Un job che sia in grado di generare, per ciascun anno, i 10 prodotti che hanno ricevuto il maggior
numero di recensioni e, per ciascuno di essi, le 5 parole con almeno 4 caratteri più frequentemente
usate nelle recensioni (campo text), indicando, per ogni parola, il numero di occorrenze della parola.

## Secondo job
Un job che sia in grado di generare una lista di utenti ordinata sulla base del loro apprezzamento, dove
l’apprezzamento di ogni utente è ottenuto dalla media dell’utilità (rapporto tra HelpfulnessNumerator
e HelpfulnessDenominator) delle recensioni che hanno scritto, indicando per ogni utente il loro
apprezzamento.

## Terzo job
Un job in grado di generare gruppi di utenti con gusti affini, dove gli utenti hanno gusti affini se hanno
recensito con score superiore o uguale a 4 almeno 3 prodotti in comune, indicando, per ciascun
gruppo, i prodotti condivisi. Il risultato deve essere ordinato in base allo UserId del primo elemento del
gruppo e non devono essere presenti duplicati.