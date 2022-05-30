#!/bin/bash

if [ -n "${LOADED_SYSTEM_COMMONS_VERSION}" ]; then
  return 0
fi

###############################################
# Displays version
###############################################
show_version() {
  echo "COMPSs version 3.0 Lavender"
  echo " "
}

###############################################
# Displays version's full header
###############################################
show_full_version() {
  cat << EOF

        .-~~-.--.           ______        ______
       :         )         |____  \\      /  __  \\
 .~ ~ -.\\       /.- ~~ .      __) |      | |  | |
 >       \`.   .'       <     |__  |      | |  | |
(         .- -.         )   ____) |   _  | |__| |
 \`- -.-~  \`- -'  ~-.- -'   |______/  |_| \\______/
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

Lavandula (common name lavender) is a genus of 47 known species of flowering plants in the mint family, Lamiaceae. It is native to the Old World and is found in Cape Verde and the Canary Islands, and from Europe across to northern and eastern Africa, the Mediterranean, southwest Asia to India
Many members of the genus are cultivated extensively in temperate climates as ornamental plants for garden and landscape use, for use as culinary herbs, and also commercially for the extraction of essential oils.
The most widely cultivated species, Lavandula angustifolia, is often referred to as lavender, and there is a color named for the shade of the flowers of this species. Lavender has been used over centuries in traditional medicine and cosmetics, and "limited clinical trials support therapeutic use of lavender for pain, hot flushes, and postnatal perineal discomfort."

Check more information:
   https://en.wikipedia.org/wiki/Lavandula

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

Lavender Martini - Cocktail

From: https://sweetlifebake.com/lavender-martini/

Author: Vianney Rodriguez

Ingredients:

    1 ounce vodka
    1/2 ounce fresh lemon juice
    1/4 ounce lavender syrup recipe follows
    1 cup water
    1 cup sugar
    1 tablespoon dried lavender buds

Instructions:

    1. To a cocktail shaker filled with ice add vodka, lemon juice and lavender syrup; shake well.
    2. Strain into a martini glass and garnish with a lemon slice.
    3. For Lavender syrup: Bring water and sugar to a boil, stirring until sugar dissolves.
    4. Remove from heat, add lavender buds and allow to infuse for 20 minutes.
    5. Strain, allow to cool completely and use to make cocktails, lemonade and mocktails.

EOF
}


LOADED_SYSTEM_COMMONS_VERSION=1
