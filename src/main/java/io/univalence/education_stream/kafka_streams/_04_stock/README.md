
Le but est remonté un flux fournissant le stock à l'assortiment e-commerce.

3 sources de données sont fournies :
* store stock : flux "temps réel" remontant les inventaires de stock magasin.
  Ces inventaires indiquent pour chaque produit dans chaque magasin la quantité de stock disponible.
  Ces inventaires incluent aussi le stock qui n'est pas à l'assortiment e-commerce
* location : flux batch (1x/h) fournissant le référentiel des magasins. Il permet de connaître pour chaque
  magasin sa bannière (banner), qui est soit HYP (pour hypermarché), soit SUP (pour supermarché) ou
  PRX (pour magasin de proximité).
* assortment : flux batch (1x/j) fournissant le référentiel des assortiments e-commerce
  (ou produits vendables sur le site e-commerce). Il donne pour chaque produit de chaque bannière la période
  de vente du produit.

# Question 1

L'objectif est en utilisant ces sources de données, fournir le stock à l'assortiment e-commerce.
* un stock est vendable sur le site e-commerce s'il est référencé dans l'assortiment e-commerce
* un stock est vendable sur le site e-commerce s'il est dans la période de vente indiqué dans l'assortiment
  correspondant

# Question 2

Le flux "location" indique pour chaque magasin une liste d'identifiants de services. Un service correspond à une
activité du magasin (magasin lui-même, drive associé, livraison à domicile...). Ces services se partagent le même
stock.

On veut en sorti le stock à l'assortiment e-commerce par service.
