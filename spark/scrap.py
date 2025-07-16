import pdfplumber, json, re, requests, os

# Mapping pour convertir les chiffres normaux en subscript
SUB = str.maketrans("0123456789", "₀₁₂₃₄₅₆₇₈₉")


def scrap_standards():
    os.makedirs('data_output/normes', exist_ok=True)
    url = "https://www.ecologie.gouv.fr/sites/default/files/documents/01_Tableau-Normes-Seuils%20r%C3%A9glementaires.pdf"
    response = requests.get(url, verify=False)
    
    if response.status_code == 200:
        with open("data_output/normes/tableau_normes.pdf","wb") as f:
            f.write(response.content)
    else:
        print("Erreur lors du téléchargement du document")

def fix_component_name(name):
    """
    Corrige le nom du composant en procédant ainsi :
      - Retire les espaces superflus autour et à l'intérieur des parenthèses.
      - Pour le contenu entre parenthèses, supprime tous les espaces,
        corrige "CH66" (ou "CH₆₆") en "C6H6" et convertit les chiffres en subscript.
    Par exemple :
      "BENZÈNE (CH66)" → "BENZÈNE (C₆H₆)"
      "PARTICULES (PM2,5)" reste "PARTICULES (PM₂,₅)"
    """
    # Retire les espaces juste après '(' et avant ')'
    name = re.sub(r"\(\s+", "(", name)
    name = re.sub(r"\s+\)", ")", name)
    
    def process_parentheses(match):
        content = match.group(1)
        # Supprime tous les espaces à l'intérieur
        content = re.sub(r"\s+", "", content)
        # Correction spécifique pour BENZÈNE : remplacer "CH66" ou "CH₆₆" par "C6H6"
        content = re.sub(r"CH66", "C6H6", content, flags=re.IGNORECASE)
        content = re.sub(r"CH₆₆", "C6H6", content, flags=re.IGNORECASE)
        # Convertit chaque chiffre normal en son équivalent en subscript
        new_content = "".join(ch.translate(SUB) if ch.isdigit() else ch for ch in content)
        return f"({new_content})"
    
    # Applique le traitement sur tout ce qui est entre parenthèses
    name = re.sub(r"\((.*?)\)", process_parentheses, name)
    return name

def extract_tables_from_pdf(pdf_path):
    """Extrait tous les tableaux du PDF en parcourant l'ensemble des pages."""
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_tables = page.extract_tables()
            if page_tables:
                tables.extend(page_tables)
    return tables

def parse_measurement(measurement_str):
    """
    Recherche toutes les occurrences d'une mesure dès qu'on voit (FR) ou (UE) dans la chaîne.
    Avant l'extraction, on normalise la chaîne en supprimant les retours à la ligne et en remplaçant
    les séquences d'espaces multiples par un espace unique.
    """
    # Normalisation : transformation des retours à la ligne en espace et suppression des espaces multiples
    measurement_str = re.sub(r"\s+", " ", measurement_str).strip()
    
    pattern = r"([\d,.]+\s*(?:µg/m³|µg/m3|mg/m³|mg/m3|ng/m³|ng/m3))\s*\((FR|UE)\)"
    matches = list(re.finditer(pattern, measurement_str))
    if matches:
        values = []
        origins = []
        for match in matches:
            value = match.group(1).strip()
            origin = match.group(2).strip()
            values.append(value)
            origins.append(origin)
        return " ; ".join(values), " ; ".join(origins)
    return measurement_str, ""


def process_table(table):
    """
    Traite un tableau extrait (liste de lignes).
    La première ligne contient le titre complet du polluant, par exemple :
      "BENZÈNE (CH)\n6 6"
    On sépare la première partie et la deuxième (indice) afin de les fusionner
    dans le titre en insérant l'indice avant la parenthèse fermante.
    Puis on applique fix_component_name pour corriger le format.
    Les lignes suivantes contiennent 3 colonnes : [indicateur, mesure, critère].
    Retourne un dictionnaire avec "nom" et la liste "informations".
    """
    if not table or len(table) < 1:
        return None

    pollutant_title = table[0][0]
    if pollutant_title:
        parts = pollutant_title.split("\n")
        main_part = parts[0].strip()
        if len(parts) > 1:
            # On récupère la partie indice, on supprime les espaces et on la garde telle quelle
            index_part = parts[1].strip().replace(" ", "")
            # Insertion de l'indice avant la parenthèse fermante dans main_part
            if ")" in main_part:
                idx = main_part.rfind(")")
                pollutant_name = main_part[:idx] + index_part + main_part[idx:]
            else:
                pollutant_name = main_part + index_part
        else:
            pollutant_name = main_part
    else:
        pollutant_name = "Inconnu"
    
    # Applique la correction pour insérer l'indice dans la parenthèse et convertir en subscript
    pollutant_name = fix_component_name(pollutant_name)

    informations = []
    last_indicator = None

    for row in table[1:]:
        indicator = row[0] if row[0] is not None else ""
        measurement = row[1] if row[1] is not None else ""
        critere = row[2] if len(row) > 2 and row[2] is not None else ""

        # Si l'indicateur est vide, on reprend le dernier non vide
        if not indicator and last_indicator:
            indicator = last_indicator
        elif indicator:
            last_indicator = indicator

        value, origin = parse_measurement(measurement)

        indicator = indicator.replace("\n", " ").strip()
        value = value.replace("\n", " ").strip()
        origin = origin.replace("\n", " ").strip()
        critere = critere.replace("\n", " ").strip()

        if value:
            informations.append({
                "indicateur": indicator,
                "valeurs": value,
                "origine": origin,
                "critere": critere
            })

    return {"nom": pollutant_name, "informations": informations}

def create_json_from_pdf(pdf_path):
    """Extrait les tableaux du PDF et génère le JSON correspondant."""
    tables = extract_tables_from_pdf(pdf_path)
    polluants = []
    for table in tables:
        parsed = process_table(table)
        if parsed:
            polluants.append(parsed)
    return {"polluants": polluants}

def get_normes_data():
    scrap_standards()
    pdf_path = "data_output/normes/tableau_normes.pdf"  # Le PDF doit être dans le même dossier que le script
    data = create_json_from_pdf(pdf_path)
    with open("data_output/normes/polluants.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return data