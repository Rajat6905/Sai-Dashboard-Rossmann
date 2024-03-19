from sqlalchemy.orm import Session
from model.model import Aisle_images

def get_aisle_images(db: Session, store_id):
    if store_id is not None:
        aisle_images = db.query(Aisle_images.images,Aisle_images.label) \
            .filter(Aisle_images.store_id == store_id) \
            .all()
    else:
        aisle_images = []
    formatted_aisle_images = [
        {
            "images": "https://rossman2.s3.eu-central-1.amazonaws.com/aisle_images/{}/{}".format(store_id,
                                                                                                     item["images"]),
            "label": item["label"]
        }
        for item in aisle_images
    ]
    return formatted_aisle_images
