# Shared league configuration — single source of truth for blog data pipeline.
# Opta competition name → blog league code mapping.
# Source this file from any script that needs the league list.

BLOG_COMP_TO_CODE <- c(
  EPL = "ENG", Championship = "ENG2", La_Liga = "ESP", Ligue_1 = "FRA",
  Bundesliga = "GER", Serie_A = "ITA", Eredivisie = "NED",
  Primeira_Liga = "POR", Scottish_Premiership = "SCO", Super_Lig = "TUR",
  UCL = "UCL", UEL = "UEL", Conference_League = "UECL"
)

BLOG_COMPS <- names(BLOG_COMP_TO_CODE)
BLOG_CODES <- unname(BLOG_COMP_TO_CODE)
