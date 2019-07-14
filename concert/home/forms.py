from django import forms

class HomeForm(forms.Form):
    post = forms.CharField(widget=forms.TextInput(
        attrs={
            'class':'form-control',
            'placeholder': 'Enter Soundcloud Username',
            'class':'col-xs-3',
        }
    ))
