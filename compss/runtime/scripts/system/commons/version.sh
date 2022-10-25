#!/bin/bash

if [ -n "${LOADED_SYSTEM_COMMONS_VERSION}" ]; then
  return 0
fi

###############################################
# Displays version
###############################################
show_version() {
  echo "COMPSs version 3.1 Margarita"
  echo " "
}

###############################################
# Displays version's full header
###############################################
show_full_version() {
  cat << EOF

        .-~~-.--.           ______        ___
       :         )         |____  \\      /   |
 .~ ~ -.\\       /.- ~~ .      __) |     /_/| |
 >       \`.   .'       <     |__  |        | |
(         .- -.         )   ____) |   _    | |
 \`- -.-~  \`- -'  ~-.- -'   |______/  |_|   |_|
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
Margarita (Bellis perennis) is a common European species of daisy, of the Asteraceae family, often considered the archetypal species of that name. Many related plants also share the name "daisy", so to distinguish this species from other daisies it is sometimes qualified as common daisy, lawn daisy or English daisy. It is a herbaceous perennial plant with short creeping rhizomes and rosettes of small rounded or spoon-shaped leaves that are from 3/4 to 2 inches (approx. 2–5 cm) long and grow flat to the ground. The flowerheads are composite, in the form of a pseudanthium, consisting of many sessile flowers about 3/4 to 1-1/4 in (approx. 2–3 cm) in diameter, with white ray florets (often tipped red) and yellow disc florets. Each inflorescence is borne on single leafless stems 3/4 - 4 in (approx. 2–10 cm), rarely 6 in (approx. 15 cm) tall. The capitulum, or disc of florets, is surrounded by two rows of green bracts known as "phyllaries". 

Check more information:
   https://en.wikipedia.org/wiki/Bellis_perennis

EOF
}

###############################################
# Displays version's drink recipe
###############################################
show_recipe() {
  cat << EOF

Margarita (Daisy) - Cocktail
   
   35 ml Tequila
   20 ml Triple Sec
   15 ml Lime Juice   

   Pour all ingredients into shaker with ice. Shake well
   and strain into cocktail glass rimmed with salt.

EOF
}


LOADED_SYSTEM_COMMONS_VERSION=1
