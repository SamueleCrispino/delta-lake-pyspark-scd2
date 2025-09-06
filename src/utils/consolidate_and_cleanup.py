#!/usr/bin/env python3
"""
consolidate_and_cleanup.py

Per ogni directory sotto BASE_DIR (es. crm_with_event_time/header/header_YYYYMMDD/):
 - trova il file CSV di output generato da Spark (part-*.csv o *.csv)
 - lo sposta/rinomina in BASE_DIR/header_YYYYMMDD.csv
 - elimina la directory originaria

Uso:
  python consolidate_and_cleanup.py --base crm_with_event_time/header

Attenzione: lo script sovrascrive il file di destinazione se esiste.
"""

import argparse
import logging
from pathlib import Path
import shutil
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def choose_best_candidate(files):
    """Sceglie il file candidato migliore:
       - preferisce file che contengono 'part' o terminano con .csv
       - se più file, sceglie il più grande (probabilmente il CSV con i dati)"""
    if not files:
        return None
    # prefer files with .csv or part in name (case insensitive)
    prioritized = [f for f in files if f.suffix.lower() == ".csv" or "part" in f.name.lower()]
    if not prioritized:
        prioritized = files
    # choose largest file (size)
    prioritized_sorted = sorted(prioritized, key=lambda p: p.stat().st_size, reverse=True)
    return prioritized_sorted[0]


def process_base_dir(base_dir: Path, dry_run: bool = False):
    if not base_dir.exists():
        logging.error("Base dir non trovata: %s", str(base_dir))
        return 1

    subdirs = sorted([p for p in base_dir.iterdir() if p.is_dir()])
    if not subdirs:
        logging.info("Nessuna sottocartella da processare in %s", base_dir)
        return 0

    for d in subdirs:
        logging.info("Processing directory: %s", d)
        # gather candidate files (non-hidden, file only)
        candidates = [f for f in d.iterdir() if f.is_file() and not f.name.startswith("_") and not f.name.startswith(".")]
        if not candidates:
            logging.warning(" - Nessun file candidato trovato in %s — salto", d)
            continue

        best = choose_best_candidate(candidates)
        if best is None:
            logging.warning(" - Nessun candidato selezionato in %s — salto", d)
            continue

        target_filename = f"{d.name}.csv"   # es: header_20230126.csv
        target_path = base_dir / target_filename
        tmp_target = base_dir / (target_filename + ".tmp")

        logging.info(" - candidato scelto: %s (size=%d bytes)", best.name, best.stat().st_size)
        logging.info(" - destinazione finale: %s", target_path)

        if dry_run:
            logging.info(" - dry-run: non eseguo spostamento o cancellazione")
            continue

        try:
            # assicurati che il file sorgente sia leggibile
            if best.stat().st_size == 0:
                logging.warning(" - file candidato ha dimensione 0 — salto")
                continue

            # sposta in tmp (stesso filesystem -> usa replace)
            # se il file è su filesystem diverso, usare shutil.copyfile e poi os.replace o os.remove
            logging.info(" - sposto %s -> %s.tmp", best, tmp_target)
            # try atomic move
            try:
                os.replace(str(best), str(tmp_target))   # sposta il file
            except OSError:
                # fallback: copy then remove
                shutil.copy2(str(best), str(tmp_target))
                best.unlink()

            # poi rinomina tmp -> final (sovrascrive se esiste)
            logging.info(" - rinomino %s -> %s (overwrite se esiste)", tmp_target, target_path)
            os.replace(str(tmp_target), str(target_path))

            # rimuovi la directory originaria (tutti i file)
            logging.info(" - rimuovo directory %s", d)
            shutil.rmtree(str(d))

            logging.info(" - OK")
        except Exception as e:
            logging.error("Errore durante il processing di %s: %s", d, e)

    logging.info("Tutte le directory processate.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Consolidate Spark CSV part files into single CSV and cleanup dirs.")
    parser.add_argument("--base", "-b", required=True, help="Base directory (es. crm_with_event_time/header)")
    parser.add_argument("--dry-run", action="store_true", help="Non eseguire azioni, mostra solo cosa farei")
    args = parser.parse_args()

    base_dir = Path(args.base).resolve()
    logging.info("Base dir: %s", base_dir)
    sys.exit(process_base_dir(base_dir, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
