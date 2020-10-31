###############################################
# Displays version
###############################################
show_version() {
  echo "COMPSs version 2.8 Iris"
  echo " "
}

###############################################
# Displays version's full header
###############################################
show_full_version() {
  cat << EOF
                             
        .-~~-.--.             _____      _______
       :         )           |____ \\    /  ____  \\
 .~ ~ -.\\       /.- ~~ .      ___) |    | (____) |
 >       \`.   .'       <     / ___/      > ____ < 
(         .- -.         )   | |____   _ | (____) | 
 \`- -.-~  \`- -'  ~-.- -'    |______| |_|\\________/
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
Iris is a perennial plant, growing from creeping rhizomes (rhizomatous irises) or, in drier climates, from bulbs (bulbous irises). It  has long, erect flowering stems which may be simple or branched, solid or hollow, and flattened or has a circular cross-section. The rhizomatous species usually have 3–10 basal sword-shaped leaves growing in dense clumps. The bulbous species have cylindrical, basal leaves. 

Check more information:
   https://en.wikipedia.org/wiki/Iris_(plant)

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

Iris - Cocktail

From: https://www.mixology.recipes/cocktails/iris-cocktail

Ingredients

   1½ oz brandy (4.5 cl)
   ½ oz sweet vermouth (1.5 cl)
   ½ oz fresh lemon juice (1.5 cl)

Instructions

    Place all ingredients in iced cocktail shaker strain. Shake and add sugar to taste. Serve in a cocktail glass (4.5 oz) 

EOF
}
