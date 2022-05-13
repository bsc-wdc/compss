#!/bin/bash

if [ -n "${LOADED_SYSTEM_COMMONS_VERSION}" ]; then
  return 0
fi

###############################################
# Displays version
###############################################
show_version() {
  echo "COMPSs version 2.10 Kumquat"
  echo " "
}

###############################################
# Displays version's full header
###############################################
show_full_version() {
  cat << EOF
                             
        .-~~-.--.             _____       _   ______
       :         )           |____ \\     / | /  __  \\
 .~ ~ -.\\       /.- ~~ .      ___) |    /_ | | |  | |
 >       \`.   .'       <     / ___/      | | | |  | |
(         .- -.         )   | |____   _  | | | |__| | 
 \`- -.-~  \`- -'  ~-.- -'    |______| |_| |_| \\______/
   (        :        )           _ _ .-:
    ~--.    :    .--~        .-~  .-~  }
        ~-.-^-.-~ \\       .~  .-~   .~
                 \\ \\      \\ '_ _ -~
                  \`.\`.    //
         . - ~ ~-.__\`.\`-.//
     .-~   . - ~  }~ ~ ~-.~-.
   .' .-~      .-~       :/~-.~-./:
  /_~_ _ . - ~                 ~-.~-._
                                   ~-.<
EOF
  show_version
}

###############################################
# Displays version's flower description
###############################################
show_flower() {
  cat << EOF
Kumquats are a group of small fruit-bearing trees in the flowering plant family Rutaceae. They were previously classified as forming the now-historical genus Fortunella, or placed within Citrus, sensu lato.
They are slow-growing evergreen shrubs or short trees that stand 2.5 to 4.5 meters (8 to 15 ft) tall, with dense branches, sometimes bearing small thorns. The leaves are dark glossy green, and the flowers are white, similar to other citrus flowers, and can be borne singly or clustered within the leaves' axils. Depending on size, the kumquat tree can produce hundreds or even thousands of fruits each year.

Check more information:
   https://en.wikipedia.org/wiki/Kumquat

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

Kumquat Quest - Cocktail 

From: https://cocktailsdistilled.com/recipe/kumquat-quest/

Ingredients:

    2 oz Vodka
    1/2 oz Grand Marnier
    1/2 oz Lime Juice
    Sugar, Demerara Brown
    3-4 Kumquats

Instructions:
    Cut all the kumquats in half and put all the rest of the ingredients into a highball glass. Muddle all and add crushed ice. Float Grand Marnier.

EOF
}


LOADED_SYSTEM_COMMONS_VERSION=1