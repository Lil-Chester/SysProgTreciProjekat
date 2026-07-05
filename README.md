Zadatak 20:



Koristeći Rx.NET i OpenMeteo API, prikupiti podatke o vremenskoj prognozi za zadatu lokaciju

i vremenski period. Rx vrši osnovno mapiranje dobijenih vremenskih podataka i emituje ih kao

poruke aktorima. Aktori čuvaju vremensku seriju kao interno stanje i ažuriraju tekuće proračune

prosečne, minimalne i maksimalne temperature, kao i UV indeksa za dati period. Web server prima

zahteve sa parametrima lokacije i vremenskog perioda i prevodi ih u poruke aktorima. Prikazati

dobijene rezultate.



Dokumentacija dostupna na linku: https://open-meteo.com/en/docs



Primer poziva serveru za Nis: http://localhost:8080/?lat=43.32\&lng=21.89\&start=2026-07-09\&end=2026-07-10

