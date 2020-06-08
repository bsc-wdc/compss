###############################################
# Displays version
###############################################
show_version() {
  echo "COMPSs version 2.7 Hyacinth"
  echo " "
}

###############################################
# Displays version's full header
###############################################
show_full_version() {
  cat << EOF
        .-~~-.--.            _____        ________   
       :         )          |____ \\     |_____  /
 .~ ~ -.\\       /.- ~~ .      ___) |         / /
 >       \`.   .'       <     / ___/         / / 
(         .- -.         )   | |____   _     / /
 \`- -.-~  \`- -'  ~-.- -'    |______| |_|  /_/ 
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
Hyacinthus is a small genus of bulbous, spring-blooming perennial, fragrant flowering plants in the family Asparagaceae, subfamily Scilloideae. These are commonly called hyacinths. The genus is native to the eastern Mediterranean. Hyacinthus grows from bulbs, each producing around four to six linear leaves and one to three spikes or racemes of flowers. In the wild species, the flowers are widely spaced with as few as two per raceme in H. litwinovii and typically six to eight in H. orientalis, which grows to a height of 15–20 cm (6–8 in). Cultivars of H. orientalis have much denser flower spikes and are generally more robust.

Check more information:
   https://en.wikipedia.org/wiki/Hyacinth_(plant)

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

Hyacinth  - Cocktail

From: http://tasteandtipple.ca/the-hyacinth-cocktail/#Hyacinth

Ingredients

    1½ tsp Blueberry Syrup recipe follows
    4 oz sparkling wine
    Fresh blueberries for garnish

Instructions

    Place Blueberry Syrup into a Champagne flute. Top with sparkling wine and garnish with 3 or 4 fresh blueberries.

Blueberry Syrup

    1 pint blueberries
    1½ tbsp sugar
    ¼ cup water

Instructions

    In a small saucepan, place all ingredients over medium heat. Bring to boil, reduce temperature to low and cook for 4 to 5 minutes until berries soften. Remove pan from heat, strain syrup and cool.

EOF
}
