
def valid_infotypes(lang):
    if lang == 'ja':
        return [ \
            'ActorActress', \
            'Infobox_Film', \
            'Infobox_animanga/Header', \
            'コンピュータゲーム', \
            'Infobox_人物', \
            'Infobox_Musician', \
            'Infobox_作家', \
        ]
    elif lang == 'en':
        return [\
            'Infobox_film', \
            'Infobox_musical_artist', \
            'Infobox_writer', \
            'Infobox_video_game', \
            'Infobox_television', \
            'Infobox_animanga/Header', \
        ]
    else:
        return []

valid_categories = [ \
]

