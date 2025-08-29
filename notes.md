# Correzioni/rafforzamenti essenziali (priorità alta)

## Confronti NULL-safe

Il confronto updates.col <> existing.col non è NULL-safe: se uno dei due è NULL la condizione restituisce NULL (cioè non vera) e la differenza non viene rilevata. Questo porta a non catturare cambi quando i valori passano da / a NULL.

Usa la null-safe equality <=> di Spark SQL oppure NOT (a <=> b):

Da sostituire nelle condizioni:
    .where(...) di newItemsToInsert 
    .whenMatchedUpdate(...) di merge condition

    "
        existing.valid_to = date('9999-12-31')
        AND (
        NOT (staged_updates.contracted_price <=> existing.contracted_price)
        OR NOT (staged_updates.total_discount <=> existing.total_discount)
        OR NOT (staged_updates.data_fine_prestazione <=> existing.data_fine_prestazione)
        )
    "

## Evitare sovrapposizioni nelle date inclusivo/esclusivo + is_current column

È molto comodo e ottimizza le query: mantieni is_current booleana che vale true per la riga attiva (valid_to = max_date). La aggiorni nel whenMatchedUpdate (set is_current=false) e nel whenNotMatchedInsert imposti is_current=true. Aiuta anche a evitare costose predicate sui timestamp in molte query.

--> da valutare se la colonna is_current ha senso per ottimizzazione partizioni! 

nella parte whenMatchedUpdate:

    "valid_to": "date_add(staged_updates.valid_from, -1)",
    "is_current": "false"


whenNotMatchedInsert --> "is_current": "true"
whenMatchedUpdate    --> "is_current": "false", "valid_to": "date_add(staged_updates.valid_from, -1"


## Deduplicazione più solida (row_number)

!!!! VERIFICARE CHE IN TUTTI I RECORD CON cod_contratto duplicato sia duplicato anche creazione_dta
=> in caso contrario aggiungere creazione_dta come chiave di deduplicazione 


La deduplica attuale conta occorrenze count(*) e filtra quelle con flag==1. Questo non ordina e non risolve quale dei record duplicati tenere (es. se parziale update con timestamp diverso). Migliore approccio: row_number() ordinando per un campo che rappresenta la versione o time (es. creazione_dta desc) e tenere solo row_number = 1.

Esempio:

from pyspark.sql.window import Window
w = Window.partitionBy(*deduplication_keys).orderBy(col("creazione_dta").desc())
df_extracted = df_extracted.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")


NOTE IMPORTANTI:
creazione_dta --> potrebbe essere sempre la stessa per ogni duplicato
valid_from    --> viene preso dal batch quindi anche in questo caso è sempre uguale a parità di batch

Potrebbe essere possibile definire una logica basata sui seguenti casi unici di status:
+--------------------------------+
|status_quote                    |
+--------------------------------+
|Rifiutata                       |
|Annullata                       |
|Scatto Bloccato                 |
|Firmata - in attesa approvazione|
|Bozza                           |
|In Firma Cartacea               |
|Presentata                      |
|In attesa scatto                |
|In Firma                        |
|Accepted                        |
|Verbal Order (VO) Registrato    |
|Other                           |
+--------------------------------+


Performance e pratiche migliori

Le funzioni window fanno shuffle sulla colonna di partizione: per performance, repartition(deduplication_keys) prima della window può essere utile.

Esegui la dedup prima delle operazioni costose (joins pesanti, calcoli aggregati): riduce dati e shuffle successivo.

Se i dataset sono molto grandi, valuta di deduplicare per file (local dedup) e poi una dedup globale.


# Casi aggiuntivi da implementare

## Soft delete / Tombstone
Se un record viene eliminato, non cancellarlo fisicamente: chiudi la riga attiva impostando valid_to = data_annullamento (o date of deletion) e/o inserisci una nuova riga “tombstone” con is_current=true ma status='DELETED' o simile. Questo preserva la storia e rende idempotenti le query point-in-time.

Detecting a DELETION:
    detect updates.causale_annullamento IS NOT NULL OR updates.status = 'CANCELLED' → nel merge, per questi casi:

    impostare existing.valid_to = updates.data_annullamento (o date_add(updates.valid_from, -1))

    inserire riga nuova con status='CANCELLED' e is_current=true (opzionale)

BUT what about if a record has been sent by error and need a technical deletion?


## Late-arriving data e overlapping ranges
Se arrivano record con valid_from antecedente alla riga attiva, devi spezzare l'intervallo esistente (es. ridurre valid_to della riga esistente) e inserire la nuova versione coerentemente. La logica merge può gestirne una parte ma per sovrapposizioni complesse potresti dover:

identificare esistenti con valid_from <= new.valid_from <= valid_to e aggiornare valid_to a date_add(new.valid_from, -1), oppure

implementare una procedura che normalizzi intervalli prima di unire.

## Schema evolution
Prevedi come gestire cambi di schema (aggiunta colonna, cambio tipo). Delta supporta schema evolution, ma nelle merge/insert devi abilitare spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") (Databricks) oppure gestire esplicitamente le colonne mancanti. Nel test 3 del prof sarà utile dimostrare: aggiungi una colonna, carica nuovi file con la colonna e mostra come fare time-travel su versioni diverse.


## Logging / audit / controllo qualità
Aggiungi colonne di audit: batch_id, ingest_ts, source_file. Salva un controllo run-table con metriche: numero record input, numero inseriti, numero chiusi, tempo di esecuzione. Questo è estremamente apprezzato in un progetto accademico.
##### --> Esegui semplici DQ checks e fallisci/avvisa in base a soglie: molto apprezzato in contesto accademico.    
##### --> integrare le metriche nuove con quelle di validation



## Idempotenza e re-run safety
Assicurati che se rilanci lo stesso job con gli stessi file non vengano create versioni duplicate. Il pattern con merge e whenNotMatchedInsert aiuta, ma attenzione alla creazione degli stagedUpdates: se stagedUpdates cambia tra esecuzioni devi garantire dedup e idempotenza (usa batch_id o file_name per identificare).


# Test e validazione (da eseguire per dimostrare correttezza)

Prepara casi di test unitari con piccoli dataset che coprano:

- update di campo (cambio prezzo) → chiusura riga vecchia e inserimento nuova.
- update con valori NULL → verifica null-safe.
- deletions → verifica tombstone/chiusura.
- late-arriving record con valid_from nel passato → verifica split degli intervalli.
- schema evolution → aggiunta colonna e query point-in-time.

Per ogni test verifica:
- non ci sono intervalli sovrapposti per la stessa chiave (assert max(valid_from) <= min(valid_to) o simile),
- esiste esattamente una riga con is_current=true per ogni chiave attiva.