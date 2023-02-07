

from pycompss.api.task import task
from pycompss.api.parameter import *

# ############ #
# Final tasks #
# ############ #


@task(images=COLLECTION_FILE_IN,
      result=FILE_OUT)
def merge_results(images, result):
    from PIL import Image
    imgs = map(Image.open, images)
    widths, heights = zip(*(i.size for i in imgs))
    total_width = sum(widths)
    max_height = max(heights)
    new_im = Image.new('RGB', (total_width, max_height))
    x_offset = 0
    for im in imgs:
        # d = ImageDraw.Draw(im)
        # d.text((10,10), "Hello World", fill="black")
        new_im.paste(im, (x_offset, 0))
        x_offset += im.size[0]
    new_im.save(result)
