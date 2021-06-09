###############################################
# Displays version
###############################################
show_version() {
  echo "COMPSs version 2.9 Jasmine"
  echo " "
}

###############################################
# Displays version's full header
###############################################
show_full_version() {
  cat << EOF
                             
        .-~~-.--.             _____      _______
       :         )           |____ \\    /  ___  \\
 .~ ~ -.\\       /.- ~~ .      ___) |    | (___) |
 >       \`.   .'       <     / ___/      \\____  | 
(         .- -.         )   | |____   _   ____) | 
 \`- -.-~  \`- -'  ~-.- -'    |______| |_| |______/
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
Jasmine is a genus of shrubs and vines in the olive family (Oleaceae). The flowers are typically around 2.5 cm (0.98 in) in diameter. They are white or yellow in color, although in rare instances they can be slightly reddish. The flowers are borne in cymose clusters with a minimum of three flowers, though they can also be solitary on the ends of branchlets. Each flower has about four to nine petals, two locules, and one to four ovules. They have two stamens with very short filaments. The bracts are linear or ovated. 
Check more information:
   https://en.wikipedia.org/wiki/Jasmine

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

Jasmine - Cocktail 

From: https://www.liquor.com/recipes/jasmine/

Ingredients:

    1 1/2 oz Gin
    1/4 oz Campari
    1/4 oz Orange liqueur
    3/4 oz Fresh lemon juice
    Garnish: Lemon twist

Instructions:
    Place all ingredients in iced cocktail shaker strain. Shake and add sugar to taste. Strain into a chilled cocktail glass. Garnish with a lemon twist

EOF
}
