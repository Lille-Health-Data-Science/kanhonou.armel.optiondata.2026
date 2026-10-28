# Projet « application diabète » · Réponses

## 1. Quelles ressources et quelles recherches FHIR ?

L'application (`projet_diabete.ipynb`) n'utilise que quatre ressources FHIR R4, lues par recherche REST :

| Besoin | Ressource | Recherche utilisée dans le notebook |
|---|---|---|
| Identifier le patient | `Patient` | `GET [base]/Patient?identifier=urn:m2dss:kanhonou\|<id>` |
| Dernières glycémies | `Observation` | `GET [base]/Observation?patient=Patient/<id>&code=http://loinc.org\|2345-7,http://loinc.org\|14749-6&_sort=-date&_count=5` |
| Traitement prescrit | `MedicationRequest` | `GET [base]/MedicationRequest?patient=Patient/<id>&status=active` |
| Traitement déclaré | `MedicationStatement` | `GET [base]/MedicationStatement?patient=Patient/<id>&status=active` |

Les glycémies sont filtrées par **code LOINC** (2345-7 en masse, 14749-6 en moles) et non par libellé, triées de la plus récente à la plus ancienne (`_sort=-date`). Les résultats arrivent dans un `Bundle` `searchset` dont on suit les liens `next` (pagination). Côté client, on écarte les mesures non `final` ou sans valeur (une mesure `cancelled` est écartée dans la démo), puis on convertit tout en mg/dL (1 g/L = 100 mg/dL ; 1 mmol/L = 18,016 mg/dL). Le patient de démonstration est créé au préalable par un `Bundle` de type `transaction` (`POST [base]`) ; l'application elle-même ne fait ensuite que des lectures.

## 2. Quel mécanisme d'authentification en production ?

Le serveur HAPI public n'a **aucune authentification** : il ne convient qu'à des données fictives. En production, on utiliserait **SMART on FHIR**, c'est-à-dire **OAuth 2.0 + OpenID Connect** :
- application lancée depuis le dossier patient (*EHR launch*) ou de façon autonome, flux *authorization code* avec **PKCE** ;
- **scopes** limités au strict besoin : `patient/Patient.r`, `patient/Observation.rs`, `patient/MedicationRequest.rs`, `patient/MedicationStatement.rs` (minimisation, RGPD) ;
- **jeton d'accès** court (*Bearer*) envoyé dans l'en-tête `Authorization`, renouvelé par *refresh token* ; échanges toujours en **TLS** ;
- pour un traitement serveur à serveur (ex. export nocturne), le profil *SMART Backend Services* (`client_credentials` avec JWT signé par clé asymétrique).

En France, l'identité du professionnel passerait par **Pro Santé Connect** (carte CPS / e-CPS), et l'accès serait tracé (ressource `AuditEvent`).

## 3. Pourquoi FHIR plutôt qu'une intégration propriétaire ?

- **Un seul code, plusieurs sources** : les recherches ci-dessus fonctionnent sur tout serveur FHIR R4 conforme (HAPI, entrepôt hospitalier, éditeur de DPI), sans connecteur spécifique ni dépendance à un fournisseur. Le `CapabilityStatement` (`/metadata`) indique ce que le serveur sait faire.
- **Sémantique partagée** : terminologies standard (LOINC pour les examens, UCUM pour les unités, RxNorm/ATC pour les médicaments, SNOMED CT pour les diagnostics). C'est grâce à elles qu'on a pu harmoniser automatiquement trois laboratoires aux unités différentes.
- **Écosystème** : sécurité standard (SMART on FHIR), export de masse (`$export`, utilisé au TP 2 vers OMOP), outils de validation, bibliothèques (`fhir.resources`).
- **Cadre réglementaire** : FHIR est le standard du CI-SIS de l'ANS, du Ségur du numérique et de l'Espace européen des données de santé.

Limite : FHIR standardise la structure, mais pas l'usage. Les laboratoires restent libres de leurs unités : l'harmonisation et les contrôles qualité restent nécessaires côté application.
