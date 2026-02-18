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
The poinsettia (Euphorbia pulcherrima) is a commercially important flowering plant species of the diverse spurge family Euphorbiaceae. Indigenous to Mexico and Central America and first described in 1834. It is particularly well known for its red and green foliage and is widely used in Christmas floral displays. Poinsettias are shrubs or small trees, with heights of 0.6 to 4 m (2.0 to 13.1 ft).

https://en.wikipedia.org/wiki/Poinsettia

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

Poinsettia - Cocktail

  Source: https://www.cocktailwave.com/recipes/poinsettia

  The Poinsettia cocktail is a classic holiday drink that has been
  enjoyed for decades. It is believed to have been created in the
  1950s, and its name is inspired by the vibrant red and green
  colors of the poinsettia plant, which is a popular decoration
  during the holiday season. This festive cocktail is perfect for
  holiday parties and gatherings, and is often enjoyed by those
  who appreciate a light, fruity, and bubbly drink.

  Ingredients:

    Champagne: 4 oz(120ml)
    Cranberry juice: 2 oz(60ml)
    Orange liqueur: 1 oz(30ml)
    Fresh cranberries: 3
    Garnish: 1 sprig of mint

  How to make:

  1 - Chill a champagne flute in the freezer for a few minutes to
      ensure the cocktail stays cold.
  2 - In a cocktail shaker filled with ice, combine the cranberry
      juice and orange liqueur. Shake well to mix.
  3 - Strain the mixture into the chilled champagne flute.
  4 - Top with the champagne, pouring slowly to avoid overflowing.
  5 - Gently drop a few fresh cranberries into the glass for added
      color and flavor.
  6 - Garnish with a sprig of mint, placing it on the rim of the
      glass or floating it on top of the cocktail.

EOF
}


LOADED_SYSTEM_COMMONS_VERSION=1
