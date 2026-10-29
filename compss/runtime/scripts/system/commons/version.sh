#!/bin/bash

if [ -n "${LOADED_SYSTEM_COMMONS_VERSION}" ]; then
  return 0
fi

VERSION=$(cat "${COMPSS_HOME}/Runtime/scripts/system/commons/VERSION")
VERSION_NAME=$(cat "${COMPSS_HOME}/Runtime/scripts/system/commons/VERSION_NAME")
VERSION_FLOWER=$(cat "${COMPSS_HOME}/Runtime/scripts/system/commons/VERSION_FLOWER")

###############################################
# Displays version
###############################################
show_version() {
  echo "COMPSs version ${VERSION} ${VERSION_NAME}"
  echo " "
}

###############################################
# Displays version's full header
###############################################
show_full_version() {
  echo "${VERSION_FLOWER}"
  show_version
}

###############################################
# Displays version's flower description
###############################################
show_flower() {
  cat << EOF
Petrea volubilis, commonly known as purple wreath, queen's wreath or sandpaper vine, is an evergreen flowering vine in the family Verbenaceae, native to tropical America, that is valued especially for its display of violet flowers. 

https://en.wikipedia.org/wiki/Petrea_volubilis

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

The Last Monsoon - Cocktail

  Source: https://flyingapronstucson.com/2022/bedeviling-shrimp-fra-diavolo/

  Created by mixologist Alfredo "Fredo" Aguilar at Zio Peppe in Tucson, Arizona,
  The Last Monsoon won the 2022 Tucson City of Gastronomy Cocktail of the Year Challenge.
  This Southwestern-inspired cocktail showcases the distinctive flavours of the Sonoran
  Desert, combining reposado tequila with a house-made pomegranate and autumn spice
  cordial, vanilla and chiltepin syrup, red wine vinegar, fresh lemon juice, and an
  aromatic creosote spritz.

  Its flavour profile balances the tart, fruity notes of pomegranate and citrus with
  warming autumn spices, subtle vanilla sweetness, and a gentle kick of chiltepin chile.
  The creosote adds a distinctive desert aroma, evoking the earthy scent of the desert
  after rainfall. Finished with a delicate lilac queen's wreath garnish, the cocktail
  offers a striking visual connection to the region's desert flora.

  Ingredients:

    La Gritona Reposado Tequila
    Housemade pomegranate & fall spice cordial
    Vanilla & chiltepin simple syrup (chiltepin is a wild-harvested desert chili pepper)
    Red wine vinegar
    Fresh lemon juice
    Creosote spritz / tincture
    Orange blossom water
    Queen's wreath

  How to make:

    1 - Combine the reposado tequila, pomegranate and autumn spice cordial,
        vanilla and chiltepin simple syrup, a dash of red wine vinegar, and
        fresh lemon juice in a cocktail shaker filled with ice.
    2 - Shake well until thoroughly chilled.
    3 - Strain into a suitable glass.
    4 - Finish with a spritz of creosote tincture and a drop of orange blossom water.
    5 - Garnish with queen's wreath.

EOF
}


LOADED_SYSTEM_COMMONS_VERSION=1
