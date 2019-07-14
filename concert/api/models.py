from django.db import models

# Create your models here.
class Matches(models.Model):
    Event = models.CharField(max_length=255)
    Date = models.DateTimeField(auto_now=True)
    Venue = models.CharField(max_length=255)
    LikedArtists = models.CharField(max_length=255)


    def __str__(self):
        return self.title
