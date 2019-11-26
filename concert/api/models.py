from django.db import models

# Create your models here.
class Matches(models.Model):
    event = models.CharField(max_length=255, default = '')
    date = models.DateTimeField(auto_now=False)
    venue = models.CharField(max_length=255,default = '')
    likedArtists = models.CharField(max_length=255,default = '')
    picLink = models.CharField(max_length=255,default = '')

    def __str__(self):
        return self.title
