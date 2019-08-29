__author__ = 'Julio Ballesteros'

import pymongo
from pymongo import MongoClient
import json
import ssl
import geopy
import geojson
import datetime


def getCityGeoJSON(adress):
    """ Devuelve las coordenadas de una direccion a partir de un str de la direccion
    Argumentos:
        adress (str) -- Direccion
    Return:
        (str) -- GeoJSON
    """
    from geopy.geocoders import Nominatim
    geopy.geocoders.options.default_user_agent = "P1_abd.py"
    #Solucion error
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    geopy.geocoders.options.default_ssl_context = ctx

    geolocator = Nominatim()
    location = geolocator.geocode(adress)

    # TODO
    # Devolver GeoJSON de tipo punto con la latitud y longitud almacenadas
    # en las variables location.latitude y location.longitude
    return geojson.Point([location.longitude, location.latitude])



class ModelCursor(object):
    """ Cursor para iterar sobre los documentos del resultado de una
    consulta. Los documentos deben ser devueltos en forma de objetos
    modelo.
    """

    def __init__(self, model_class, command_cursor):
        """ Inicializa ModelCursor
        Argumentos:
            model_class (class) -- Clase para crear los modelos del
            documento que se itera.
            command_cursor (CommandCursor) -- Cursor de pymongo
        """
        # TODO
        self.model_class = model_class
        self.command_cursor = command_cursor

    def next(self):
        """ Devuelve el siguiente documento en forma de modelo
        """
        if self.alive:
            object = self.command_cursor.next()
            return self.model_class(**object)


    @property
    def alive(self):
        """True si existen más modelos por devolver, False en caso contrario
        """
        # TODO
        return self.command_cursor.alive


class Model(object):
    """ Prototipo de la clase modelo
        Copiar y pegar tantas veces como modelos se deseen crear (cambiando
        el nombre Model, por la entidad correspondiente), o bien crear tantas
        clases como modelos se deseen que hereden de esta clase. Este segundo
        metodo puede resultar mas complejo
    """
    required_vars = []
    #admissible_vars contiene todas las requiered_vars
    admissible_vars = []
    geojson_vars = []
    db = None


    """
        Constructor de la instancia, comprueba que la nueva instancia
        tiene los argumentos necesarios y todos son admisibles
    """
    def __init__(self, **kwargs):
        # TODO
        self.var_mod = []
        self._id = None
        #El **kwards es un diccionario desempaquetado para pasarselo llamando a la funcion con **dict

        #Comprobar que tiene todos los argumentos necesarios ( estan los requiered_vars)
        if not all(elem in kwargs.keys() for elem in self.__class__.required_vars):
            raise ValueError('Se ha introducido una variable no valida.')

        #Comprobar que los demas argumentos son admisibles (estan en admissible_vars)
        if not all(elem in self.admissible_vars for elem in kwargs.keys()):
            raise ValueError('Se ha introducido una variable no valida.')

        #Convertir atributos a geojson
        for var in self.geojson_vars:
            if var in kwargs.keys():
                kwargs[var] = getCityGeoJSON(kwargs[var])

        #Crear el objeto
        self.__dict__.update(kwargs)


    """
        El metodo save guarda en la db los argumentos medificados,
        que se han apuntado en var_mod, si no tiene id se guarda todo
    """
    def save(self):
        # TODO

        #si no tiene id se guarda nuevo entero
        if self._id == None:
            print('adios')
            dicc = self.__dict__.copy()
            dicc.pop('_id')
            dicc.pop('var_mod')
            self._id = self.db.insert_one(dicc).inserted_id
            self.var_mod = []

        #comprobar las variables modificadas var_mod
        #actualizar en db con consulta update las variables modificadas
        else:
            if len(self.var_mod) > 0:
                dicc = {}
                for var in self.var_mod:
                    dicc[var] = self.__dict__[var]
                query = {'_id': self._id}
                self.db.update(query, dicc)
                self.var_mod = []



    """
        El metodo update actualiza la instancia del objeto con nuevos
        argumentos, los cuales tiene que comprobarse que son admisibles,
        y apuntar los argumentos modificados
    """
    def update(self, **kwargs):
        # TODO
        #El **kwards es un diccionario desempaquetado para pasarselo llamando a la funcion con **dict
        #comprobar que los kwards son admitidos (admissible_vars)
        if not all(elem in self.admissible_vars for elem in kwargs.keys()):
            raise ValueError('Se ha introducido una variable no valida.')

        #cambiar las variables indicadas
        self.__dict__.update(kwargs)

        # Convertir atributos a geojson
        for var in self.geojson_vars:
            if var in kwargs.keys():
                self.__dict__[var] = getCityGeoJSON(self.__dict__[var])

        #apuntar las variables cambiadas en var_mod
        for key in kwargs.keys():
            if key not in self.var_mod:
                self.var_mod.append(key)

    """
        Realiza consultas sobre la base de datos
    """
    @classmethod
    def query(cls, query):
        """ Devuelve un cursor de modelos
        """
        # TODO
        # cls() es el constructor de esta clase
        #Realizar la consulta
        cursor = cls.db.aggregate(query)
        #Crear el objeto cursor y devolverlo
        model_cursor = ModelCursor(cls, cursor)
        return model_cursor


    @classmethod
    def init_class(cls, db, vars_path="model_vars.json"):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            db (MongoClient) -- Conexion a la base de datos.
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """
        # TODO
        # cls() es el constructor de esta clase

        #Conecta con la bd
        cls.db = db

        #Obtener los datos json del fichero
        with open(vars_path, 'r') as json_data:
            data = json.load(json_data)
            json_data.close()

        #Inicializar las variables required_vars, admissible_vars y geojson_vars
        for model_data in data:
            if cls.__name__ == model_data["model_name"]:
                cls.required_vars = model_data["required_vars"].copy()
                cls.admissible_vars = model_data["admissible_vars"].copy()
                cls.geojson_vars = model_data["geoJSON_vars"].copy()

"""
    Implementación de los modelos concretos heredando de la clase model
"""
class Client(Model):
    pass

class Product(Model):
    pass

class Purchase(Model):
    #permite actualizar la entrada de modo que se le asigne a cada producto el almacén más cercano al cliente.
    def allocate(self):
        pass

class Suplier(Model):
    pass

# Q1: Listado de todas las compras de un cliente
#db.purchases.aggregate()
client_name_Q1 = 'Julio'
Q1 = [
	{'$match': {'client': client_name_Q1}}
]

# Q2: Listado de todos los proveedores para un producto
#db.products.aggregate()
product_name = 'Libro'
Q2 = [
	{'$match': {'name': product_name}},
	{'$project': {'_id': 0, 'suppliers': 1}}
]

# Q3: Listado de todos los productos diferentes comprados por un cliente
#db.purchases.aggregate()
client_name_Q3 = 'Julio'
Q3 = [
	{'$match': {'client': client_name_Q3}},
	{'$group': { '_id': '$client', 'products': { '$push': '$products'}}},
	{'$project': {'_id': 0, 'products': { '$reduce': { 'input': '$products', 'initialValue': [],'in': { '$setUnion': ['$$value', '$$this'] }}}}},
	{'$unwind': '$products'},
	{'$lookup': {'from': 'products', 'localField': 'products', 'foreignField': 'name', 'as': 'product_info'}},
	{'$project': {'product_info': 1}}
]

# Q4: Calcular el peso y volumen total de los productos comprados por un cliente un día determinado
#db.purchases.aggregate()
client_name_Q4 = 'Julio'
day_of_purchase = datetime.datetime(2015, 5, 18)
Q4 = [
	{'$match': {'client': 'Ernesto', 'purchase_date': day_of_purchase}},
	{'$group': {'_id': '$client', 'products': { '$push': '$products'}}},
	{'$project': {'_id': 0, 'products': { '$reduce': { 'input': '$products', 'initialValue': [],'in': { '$setUnion': ['$$value', '$$this'] }}}}},
	{'$unwind': '$products' },
	{'$lookup': {'from': 'products', 'localField': 'products', 'foreignField': 'name', 'as': 'product'}},
	{'$project': {'_id': 0, 'product': 1}},
	{'$unwind': '$product' },
	{'$group':{'_id':0, 'total_weight':{'$sum':'$product.weight'}, 'total_volume': {'$push': {'$multiply': ['$product.dimensions.x', '$product.dimensions.y', '$product.dimensions.z']}}}},
	{'$unwind': '$total_volume'},
	{'$group': {'_id': 0, 'total_weight': {'$max':'$total_weight'}, 'total_volume': {'$sum':'$total_volume'}}}
]

# Q5: Calcular el número medio de envíos por mes y almacén
#db.purchases.aggregate()
Q5 = [
	{'$project': {'products': 1, 'year': {'$year': '$purchase_date'}, 'month': {'$month': '$purchase_date'}}},
	{'$unwind': '$products'},
	{'$lookup': {'from': 'products', 'localField': 'products', 'foreignField': 'name', 'as': 'product'}},
	{'$group': {'_id': {'year': '$year', 'month': '$month', 'suplier':'$product.suplier'}, 'total_orders': {'$sum': 1}}},
	{'$group': {'_id': {'month': '$_id.month', 'suplier':'$_id.suplier'}, 'avg_orders': {'$avg': '$total_orders'}}}
]

# Q6: Listado con los tres proveedores con más volumen de facturación. Mostrar proveedor y volumen de facturación
#db.supliers.aggregate()
Q6 = [
	{'$unwind': '$products'},
	{'$lookup': {'from': 'products', 'localField': 'products', 'foreignField':'name', 'as': 'product'}},
	{'$unwind': '$product'},
	{'$group': {'_id': '$product.suplier', 'billing_volume': {'$sum': '$product.price'}}},
	{'$sort': {'billing_volume' : -1}},
	{'$limit': 3}
]

# Q7: Listado de almacenes cerca de unas coordenadas determinadas (100km de distancia máxima) ordenadas por orden de distancia
#db.supliers.aggregate()
Q7 = [
 	{'$geoNear': {
        'near': { 'type': 'Point', 'coordinates': [-3.692457, 40.434140]},
        'distanceField': 'dist.calculated',
        'maxDistance': 100000,
        'includeLocs': 'warehouse_address',
        'spherical': 'true'
     }}
]

# Q8: Listado de compras con destino dentro de un polígono cuyos vértices vienen definidos por coordenadas
#db.purchases.find()
Q8 = {
        'mail_address':
		{
            '$geoWithin':
			{
                '$geometry':
                 {
                     "type" : "Polygon", "coordinates": [[[ -3.714856, 40.424568 ], [ -3.678243, 40.424259 ], [ -3.723849, 40.408090 ],[ -3.685620, 40.405587 ]]]
                 }
            }
        }
      }

# Q9: Guardar en una tabla nueva el listado de compras que tienen que ser enviados desde un almacén en un día determinado
#db.purchases.aggregate
day_of_orders = datetime.datetime(2015, 5, 18)
Q9 = [
	{'$match': {'purchase_date': day_of_orders}},
	{'$unwind': '$products'},
	{'$lookup': {'from': 'products', 'localField': 'products', 'foreignField':'name', 'as': 'product'}},
	{'$match': {'product.suplier': 'Luis'}},
	{'$project': {'client': 1, 'products': 1, 'purchase_date': 1, 'product.suplier': 1}}
]

if __name__ == '__main__':
    # TODO

    #Establecemos la conexion con la bd
    client = MongoClient('localhost', 27017)
    db = client.test_db

    #Inicializamos los modelos
    Client.init_class(db.clients)
    Product.init_class(db.products)
    Purchase.init_class(db.purchases)
    Suplier.init_class(db.supliers)

    #Creamos los indices
    db.clients.create_index([('mail_address', pymongo.GEOSPHERE)])
    db.purchases.create_index([('mail_address', pymongo.GEOSPHERE)])
    db.supliers.create_index([('warehouse_address', pymongo.GEOSPHERE)])




