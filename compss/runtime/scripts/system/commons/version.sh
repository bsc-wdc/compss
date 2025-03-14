#!/bin/bash

if [ -n "${LOADED_SYSTEM_COMMONS_VERSION}" ]; then
  return 0
fi

VERSION=$(cat "${COMPSS_HOME}Runtime/scripts/system/commons/VERSION")
VERSION_NAME=$(cat "${COMPSS_HOME}Runtime/scripts/system/commons/VERSION_NAME")
VERSION_FLOWER=$(cat "${COMPSS_HOME}Runtime/scripts/system/commons/VERSION_FLOWER")

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
Orchids are plants that belong to the family Orchidaceae, a diverse and widespread group of flowering plants with blooms that are often colourful and fragrant. Orchids are cosmopolitan plants that are found in almost every habitat on Earth except glaciers. The world's richest diversity of orchid genera and species is found in the tropics.

Check more information:
   https://en.wikipedia.org/wiki/Orchid

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

Black Orchid - Cocktail

  Source: https://www.greygoose.com/en-gl/cocktails/l-orange/black-orchid.html

  The black orchid martini drink is a distinctive and elegant mixture of
  GREY GOOSE® L'Orange Flavoured Vodka, fresh lemon, crème de violette,
  simple syrup, and peach bitters.

  50 ml   GREY GOOSE® L'Orange Flavoured Vodka
  30 ml   Fresh Lemon Juice
  20 ml   Crème De Violette®
  20 ml   Simple Syrup
  +       Dash of Peach Bitters
  +       Edible Orchid

  How to make:

  1 - Add all ingredients to a cocktail shaker filled with ice and shake.
  2 - Strain into a chilled martini glass.
  3 - Garnish with an edible orchid.

EOF
}


LOADED_SYSTEM_COMMONS_VERSION=1
