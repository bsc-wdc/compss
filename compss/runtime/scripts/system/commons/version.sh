#!/bin/bash

if [ -n "${LOADED_SYSTEM_COMMONS_VERSION}" ]; then
  return 0
fi

###############################################
# Displays version
###############################################
show_version() {
  echo "COMPSs version 3.2 Narcissus"
  echo " "
}

###############################################
# Displays version's full header
###############################################
show_full_version() {
  cat << EOF

        .-~~-.--.           ______        ____
       :         )         |____  \\      |__  \\
 .~ ~ -.\\       /.- ~~ .      __) |         ) |
 >       \`.   .'       <     |__  |        / /
(         .- -.         )   ____) |   _   / /__
 \`- -.-~  \`- -'  ~-.- -'   |______/  |_| |_____|
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
Narcissus is a genus of predominantly spring flowering perennial plants of the amaryllis family, Amaryllidaceae. Various common names including daffodil, narcissus, and jonquil are used to describe all or some members of the genus. Narcissus has conspicuous flowers with six petal-like tepals surmounted by a cup- or trumpet-shaped corona. The flowers are generally white and yellow (also orange or pink in garden varieties), with either uniform or contrasting coloured tepals and corona.

Check more information:
   https://en.wikipedia.org/wiki/Narcissus_(plant)

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

Spritzus Narcissus - Cocktail

   1 ounce Giffard Wild Elderflower
   ¼ ounce Garalis retsina (or other craft retsina)
   1 dash absinthe
   ¼ ounce lemon juice
   1 ½ ounces Perrier
   3 ounces Domaine Karanika Amyntaion Brut (or other Champagne-method dry sparkling wine)

   Combine all ingredients except Perrier and sparkling wine in a chilled wine glass, then add ice.
   Top wine glass with Perrier and sparkling wine.
   Garnish with grapefruit peel, spiraled into a flower.

EOF
}


LOADED_SYSTEM_COMMONS_VERSION=1
