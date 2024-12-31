# Project documentation

## Database structure

Database model is very basic. It has only one table, `places`, which contains the following columns:

- `place_id`: The unique identifier for each place.
- `place_name`: The name of the place.
- `place_type`: The type of the place.
- `latitude`: The latitude of the place.
- `longitude`: The longitude of the place.
- `parent_id`: The unique identifier of the parent place.
- `alternate_names`: A list of alternate names for the place.
- `created_at`: The date and time when the place was created.
- `updated_at`: The date and time when the place was last updated.

## Data sources

### TGN

TGN is the The Getty Vocabularies. It contains data about places, people, and things.

### HGIS de las Indias

Created by Werner Stangl: "The lugares (settlements) in the Historical-Geographic Information System (HGIS) of the Indies (1701-1808), a project of the University of Graz, funded with the support of the Fund for Scientific Investigations of the Republic of Austria (FWF) between 2015 and 2017. The general purpose of HGIS de las Indias is to create a visualization of the historical geography for the Bourbon Spanish America through time, to create a solid systematics of the administrative-territorial structure as an interface for associated projects, as well as to promote geographic methods and digital tools in Hispano-Americanist historiography."

### Raw data

Raw data is not included in the repository. It is available in the following links:

- [TGN](http://tgndownloads.getty.edu/) 
    - Dataset used: http://tgndownloads.getty.edu/VocabData/tgn_xml_0521.zip (updated 6/11/2021)
- [HGIS de las Indias](https://whgazetteer.org/datasets/14/places) Linked Places format (LPF) (updated 14/12/2024)

