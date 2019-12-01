# Generated by Django 2.2.7 on 2019-11-30 17:00

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Matches',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('event', models.CharField(default='', max_length=255)),
                ('date', models.DateTimeField()),
                ('venue', models.CharField(default='', max_length=255)),
                ('likedArtists', models.CharField(default='', max_length=255)),
                ('picLink', models.CharField(default='', max_length=255)),
            ],
        ),
    ]
