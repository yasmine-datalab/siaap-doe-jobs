import json
from neomodel import (
    StructuredNode,
    StringProperty,
    JSONProperty,
    DateTimeProperty,
    RelationshipFrom,
)


### Usine Models
class Usine(StructuredNode):
    name = StringProperty(unique_index=True)
    location = StringProperty(required=True)
    created_at = StringProperty(required=True)
    action_area = StringProperty(required=True)
    capacity = StringProperty(required=True)
    description = StringProperty(required=True)
    image = StringProperty()
    doe_folder = StringProperty()
    update_at = DateTimeProperty(default_now=True)


### Schema Models
class SchemaPID(StructuredNode):
    name = StringProperty(required=True, unique_index=True)
    schema_file_name = StringProperty(unique_index=True)
    schema_file_type = StringProperty(required=True)
    schema_file_full_path = StringProperty(required=True)
    update_at = DateTimeProperty(default_now=True)
    usine_node = RelationshipFrom(Usine, "FROM")


### Equipement Models
class Document(StructuredNode):
    name = StringProperty(unique_index=True)
    type = StringProperty(required=True)
    update_at = DateTimeProperty(default_now=True)


class Famille(StructuredNode):
    name = StringProperty(required=True, unique_index=True)
    update_at = DateTimeProperty(default_now=True)
    usine_node = RelationshipFrom(Usine, "FROM")


class Tag(StructuredNode):
    name = StringProperty(required=True, unique_index=True)  # Equivalent to Repere
    update_at = DateTimeProperty(default_now=True)
    usine_node = RelationshipFrom(Usine, "FROM")


class Equipement(StructuredNode):
    repere = StringProperty(required=True, unique_index=True)
    libelle_principal = StringProperty(required=True)
    source = JSONProperty(required=True)
    designation = StringProperty(required=True)
    fonction = StringProperty()
    founisseur = StringProperty()
    reference_founisseur = StringProperty()
    specification = StringProperty()
    commande = StringProperty()
    localisation_geographique = StringProperty()
    nom_procede = StringProperty()
    observation = StringProperty()
    mis_a_jour = StringProperty()
    full_json = JSONProperty(required=True)
    update_at = DateTimeProperty(default_now=True)
    n_pid = StringProperty()
    famille_node = RelationshipFrom(Famille, "FROM")
    document = RelationshipFrom(Document, "FROM")
    schema_pid = RelationshipFrom(SchemaPID, "FROM")
    tag = RelationshipFrom(Tag, "FROM")


# Function to process equipement
def process_equipement_data(data: dict, usine_node: Usine):
    # Create Document if not existing
    document = Document.create_or_update(
        {
            "name": data.get("SOURCE").get("Nom"),
            "type": data.get("SOURCE").get("Type"),
        }
    )
    document[0].save()

    # Create Famille if not exsiting
    famille = Famille.create_or_update(
        {
            "name": data.get("INFORMATIONS DE BASE").get("Famille"),
        }
    )
    famille[0].save()
    famille[0].usine_node.connect(usine_node)  ## Connect Famille to Usine

    # Get SchemaPID or break if not found
    schema_pid = SchemaPID.nodes.first(name=data.get("INFORMATIONS DE BASE").get("N° PID"))

    # Create Equipement if not exsiting
    equipement = Equipement.create_or_update(
        {
            "libelle_principal": data.get("LIBELLE PRINCIPAL", data.get("LIBELLÉ PRINCIPAL", "")),
            "source": data.get("SOURCE"),
            "repere": data.get("INFORMATIONS DE BASE").get("Repère"),
            "n_pid": data.get("INFORMATIONS DE BASE").get("N° PID"),
            "designation": data.get("INFORMATIONS DE BASE").get("Désignation"),
            "fonction": data.get("INFORMATIONS DE BASE").get("Fonction"),
            "founisseur": data.get("INFORMATIONS DE BASE").get("Fournisseur"),
            "reference_founisseur": data.get("INFORMATIONS DE BASE").get("Référence fournisseur"),
            "specification": data.get("INFORMATIONS DE BASE").get("Spécification"),
            "commande": data.get("INFORMATIONS DE BASE").get("Commande"),
            "localisation_geographique": data.get("INFORMATIONS DE BASE").get("Localisation Géographique"),
            "nom_procede": data.get("INFORMATIONS DE BASE").get("Nom procédé"),
            "observation": data.get("INFORMATIONS DE BASE").get("Observation"),
            "mis_a_jour": data.get("INFORMATIONS DE BASE").get("Mise à jour"),
            "full_json": json.dumps(data),
        }
    )
    equipement[0].save()
    equipement[0].document.connect(document[0])
    equipement[0].famille_node.connect(famille[0])
    equipement[0].schema_pid.connect(schema_pid)

    # Get of Create Tag
    try:
        tag = Tag.nodes.first(name=data.get("INFORMATIONS DE BASE").get("Repère"))
    except Tag.DoesNotExist:
        tag = Tag(name=data.get("INFORMATIONS DE BASE").get("Repère"))
        tag.save()
        tag.usine_node.connect(usine_node)
        print(f'Created Tag: {data.get("INFORMATIONS DE BASE").get("Repère")}')

    # Attached Equipement to Tag
    equipement[0].tag.disconnect_all()
    equipement[0].tag.connect(tag)


# Function to process Schema files
def process_schema_file(schema_pid: str, filename: str, full_path: str, usine_node: Usine):
    # Create SchemaPID if not exsiting
    schema_pid = SchemaPID.create_or_update(
        {
            "name": schema_pid,
            "schema_file_name": filename,
            "schema_file_type": filename.split(".")[-1],
            "schema_file_full_path": full_path,
        }
    )
    schema_pid[0].save()
    schema_pid[0].usine_node.connect(usine_node)  ## Connect SchemaPID to Usine


# Function to process Factories
def process_factory_data(data: dict):
    # Create Factory if not existing
    usine = Usine.create_or_update(
        {
            "name": data.get("name"),
            "location": data.get("location"),
            "created_at": data.get("created_at"),
            "action_area": data.get("action_area"),
            "capacity": data.get("capacity"),
            "description": data.get("description"),
            "image": data.get("image"),
            "doe_folder": data.get("doe_folder"),
        }
    )
    usine[0].save()
# Extract PId from file Schema filename "1115 00 00X00-00 000000 Si Pid 001 C.pdf"
def get_pid_from_filename(filename: str):
    """Extract PId from file Schema filename"""
    file_splitted_list = filename.split()
    for i, v in enumerate(file_splitted_list):
        if v.lower() == "si":
            return " ".join(file_splitted_list[i : i + 3])