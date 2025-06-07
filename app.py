# 1. IMPORTS ET CONFIGURATION INITIALE
"""
Les imports incluent :
- datetime : pour la gestion des dates
- tkinter : pour l'interface graphique
- joblib : pour charger les modèles ML
- pandas : pour la manipulation des données
- numpy : pour les calculs numériques
- sklearn : pour le prétraitement des données
- PIL : pour la gestion des images
"""
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Label
import joblib
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from PIL import Image, ImageTk
from model_Predit import StepwiseRegression
import os

# 2. CLASSE ModernStyle
class ModernStyle:

    """
    Définit l'apparence visuelle de l'application avec :
    - Un système de couleurs cohérent (basé sur les couleurs de l'Université de Lorraine)
    - Des polices standardisées
    - Des styles de boutons et d'entrées uniformes
    - Des constantes de mise en page (padding, etc.)
    """


    # Couleurs modernes
    PRIMARY_COLOR = "#0ca0a8"  # Bleu principal UNIVERSITE LORRAINE
    SECONDARY_COLOR = "#03A9F4"  # Bleu secondaire
    BACKGROUND_COLOR = "#FFFFFF"  # Fond blanc
    FRAME_BG = "#1a237e"  # Gris très clair
    TEXT_COLOR = "#1a237e"  # Texte principal
    ACCENT_COLOR = "#1a237e"  # Accent
    SUCCESS_COLOR = "#4CAF50"  # Succès
    WARNING_COLOR = "#FF9800"  # Avertissement
    ERROR_COLOR = "#F44336"  # Erreur
    HOVER_COLOR = "#283593"  # Survol
    BUTTON_COLOR = "#1a237e"  # Bleu foncé
    BUTTON_HOVER = "#283593"  # Bleu foncé légèrement plus clair pour le survol

    #BUTTON_COLOR = "#1a237e"  # Bleu foncé
    #BUTTON_HOVER = "#283593"  # Bleu foncé légèrement plus clair pour le survol
    
    # Police et tailles
    LARGE_FONT = ("Arial", 12, "bold")
    MEDIUM_FONT = ("Arial", 10, "bold")
    SMALL_FONT = ("Arial", 10, "bold")
    TITLE_FONT = ("Arial", 14, "bold")
    
    # Styles des widgets
    BUTTON_STYLE = {
        'font': MEDIUM_FONT,
        'borderwidth': 0,
        'relief': 'flat',
        'padx': 10,
        'pady': 5
    }
    
    ENTRY_STYLE = {
        'font': MEDIUM_FONT,
        'borderwidth': 1,
        'relief': 'solid'
    }
    
    # Dimensions
    PADDING = 3
    LARGE_PADDING = 8

# 3. CLASSE SystemePrediction
class SystemePrediction:
    """
Classe principale qui gère toute l'application. Ses composants principaux sont :

a) __init__
- Initialise la fenêtre principale
- Configure les variables d'instance
- Charge le logo
- Initialise les modèles ML

b) configurer_style
- Configure l'apparence des widgets Tkinter
- Définit les styles pour les différents éléments de l'interface

c) creer_interface
- Crée la structure de l'interface utilisateur
- Organise les éléments en sections :
  * En-tête avec date et utilisateur
  * Logo
  * Zone de sélection de fichier
  * Boutons d'action
  * Tableau des résultats
  * Zone de statistiques

d) Méthodes de traitement
- parcourir_fichier : Gère la sélection des fichiers
- predire : Effectue les prédictions avec les modèles ML
  * Charge et vérifie les données
  * Prétraite les données
  * Applique les modèles de prédiction
  * Affiche les résultats

e) Méthodes d'affichage et d'export
- afficher_resultats : Met à jour l'interface avec les prédictions
- exporter_resultats : Sauvegarde les résultats en Excel
- effacer_resultats : Réinitialise l'interface
"""
    def __init__(self, root):
        self.root = root
        self.root.title("(^_^)***--EduPredict v1.0--***(^_^)")
        self.root.geometry("1200x800")
        self.root.configure(bg=ModernStyle.BACKGROUND_COLOR)
        
        # Variables d'instance
        self.fichier_path = tk.StringVar()
        self.predictions_note = None
        self.predictions_reussite = None
        self.selected_features = []
        self.pseudos = None
        
        # Date et utilisateur fixes
        self.date_utc = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.utilisateur = "FETHI_KHLIFI"
        
        # Configuration du style
        self.configurer_style()
        
        # Chargement du logo
        try:
            self.logo_image = Image.open("logo.jpg")
            self.logo_image = self.logo_image.resize((120, 120), Image.LANCZOS)
            self.logo_photo = ImageTk.PhotoImage(self.logo_image)
        except Exception as e:
            print(f"Erreur lors du chargement du logo : {e}")
            self.logo_photo = None
        
        # Chargement des modèles
        self.charger_modeles()
        
        # Création de l'interface
        self.creer_interface()

    def configurer_style(self):
        style = ttk.Style()
        
        # Définition du bleu foncé pour les boutons
       
        # Configuration des styles de base
        style.configure("MainFrame.TFrame",
                      background=ModernStyle.BACKGROUND_COLOR)
        
        style.configure("Header.TLabel",
                      background=ModernStyle.BACKGROUND_COLOR,
                      foreground=ModernStyle.PRIMARY_COLOR,
                      font=ModernStyle.TITLE_FONT,
                      padding=ModernStyle.PADDING)
        
        style.configure("Info.TLabel",
                      background=ModernStyle.BACKGROUND_COLOR,
                      foreground=ModernStyle.SECONDARY_COLOR,
                      font=ModernStyle.MEDIUM_FONT)
        
        style.configure("Custom.TButton",
                      background=ModernStyle.PRIMARY_COLOR,
                      foreground=ModernStyle.BUTTON_COLOR,
                      font=ModernStyle.MEDIUM_FONT,
                      padding=(ModernStyle.PADDING, ModernStyle.PADDING))
        
        # Style pour Treeview
        style.configure("Custom.Treeview",
                      background=ModernStyle.BACKGROUND_COLOR,
                      fieldbackground=ModernStyle.BACKGROUND_COLOR,
                      foreground=ModernStyle.TEXT_COLOR,
                      rowheight=25,
                      font=ModernStyle.MEDIUM_FONT)
        
        style.configure("Custom.Treeview.Heading",
                      background=ModernStyle.PRIMARY_COLOR,
                      foreground=ModernStyle.BUTTON_COLOR,
                      font=ModernStyle.MEDIUM_FONT,
                      padding=ModernStyle.PADDING)

    def get_date_info(self):
        return f"""Date et Heure : {self.date_utc}
Nom d'utilisateur : {self.utilisateur}"""
    def charger_modeles(self):
        """Charge les modèles de prédiction"""
        try:
            self.model_note = joblib.load('model_note.pkl')
            self.model_reussite = joblib.load('model_reussite.pkl')
            self.scaler_reussite = joblib.load('scaler_reussite.pkl')
            self.selected_features = joblib.load('scaler_selected_features.pkl')
            self.modeles_charges = True
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur lors du chargement des modèles : {str(e)}")
            self.modeles_charges = False
    def creer_interface(self):
        # Frame principale
        main_frame = ttk.Frame(self.root, style="MainFrame.TFrame")
        main_frame.grid(row=0, column=0, sticky="nsew", padx=ModernStyle.LARGE_PADDING, 
                       pady=ModernStyle.LARGE_PADDING)
        
        # En-tête avec date et utilisateur
        header_info = ttk.Label(main_frame, 
                              text=self.get_date_info(),
                              style="Info.TLabel",
                              justify="left")
        header_info.grid(row=0, column=0, columnspan=2, sticky="w", 
                        pady=(0, ModernStyle.PADDING))
        
        # Logo à droite
        if self.logo_photo:
            logo_frame = ttk.Frame(main_frame, style="MainFrame.TFrame")
            logo_frame.grid(row=0, column=2, rowspan=2, sticky="ne")
            logo_label = Label(logo_frame, 
                             image=self.logo_photo,
                             bg=ModernStyle.BACKGROUND_COLOR)
            logo_label.grid(row=0, column=0)
        
        # Titre de l'université
        univ_title = ttk.Label(main_frame,
                              text="Université de Lorraine\nInstitut des Sciences du Digital,\nManagement et Cognition",
                              style="Header.TLabel",
                              justify="center")
        univ_title.grid(row=1, column=0, columnspan=3, pady=ModernStyle.LARGE_PADDING)
        
        # Zone de sélection de fichier avec style moderne
        file_frame = ttk.LabelFrame(main_frame, 
                                  text="Sélection du fichier",
                                  style="MainFrame.TFrame",
                                  padding=ModernStyle.PADDING)
        file_frame.grid(row=2, column=0, columnspan=3, sticky="ew",
                       pady=ModernStyle.PADDING)
        
        entry = ttk.Entry(file_frame, 
                         textvariable=self.fichier_path,
                         width=70,
                         font=ModernStyle.MEDIUM_FONT)
        entry.grid(row=0, column=0, padx=(ModernStyle.PADDING, 0))
        
        browse_btn = ttk.Button(file_frame,
                              text="Parcourir",
                              command=self.parcourir_fichier,
                              style="Custom.TButton")
        browse_btn.grid(row=0, column=1, padx=ModernStyle.PADDING)
        
        # Boutons d'action
        btn_frame = ttk.Frame(main_frame, style="MainFrame.TFrame")
        btn_frame.grid(row=3, column=0, columnspan=3, pady=ModernStyle.LARGE_PADDING)
        
        predict_btn = ttk.Button(btn_frame,
                               text="Prédire",
                               command=self.predire,
                               style="Custom.TButton")
        predict_btn.grid(row=0, column=0, padx=ModernStyle.PADDING)
        
        export_btn = ttk.Button(btn_frame,
                              text="Exporter",
                              command=self.exporter_resultats,
                              style="Custom.TButton")
        export_btn.grid(row=0, column=1, padx=ModernStyle.PADDING)
        
        clear_btn = ttk.Button(btn_frame,
                             text="Effacer",
                             command=self.effacer_resultats,
                             style="Custom.TButton")
        clear_btn.grid(row=0, column=2, padx=ModernStyle.PADDING)
        
        # Tableau des résultats
        results_frame = ttk.LabelFrame(main_frame,
                                     text="Résultats des prédictions",
                                     style="MainFrame.TFrame",
                                     padding=ModernStyle.PADDING)
        results_frame.grid(row=4, column=0, columnspan=3, sticky="nsew",
                         pady=ModernStyle.PADDING)
        
        # Configuration du Treeview
        self.tree = ttk.Treeview(results_frame,
                                columns=("pseudo", "note", "reussite"),
                                show="headings",
                                style="Custom.Treeview")
        
        self.tree.heading("pseudo", text="Pseudo")
        self.tree.heading("note", text="Note Prédite (/20)")
        self.tree.heading("reussite", text="Statut")
        
        self.tree.column("pseudo", width=200)
        self.tree.column("note", width=150)
        self.tree.column("reussite", width=200)
        
        # Scrollbar pour le Treeview
        scrollbar = ttk.Scrollbar(results_frame,
                                orient="vertical",
                                command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Zone de statistiques
        self.stats_label = ttk.Label(main_frame,
                                   text="",
                                   style="Info.TLabel",
                                   justify="left")
        self.stats_label.grid(row=5, column=0, columnspan=3, sticky="w",
                            pady=ModernStyle.PADDING)
        
        # Configuration du redimensionnement
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)

    def parcourir_fichier(self):
   # """Ouvre la boîte de dialogue pour sélectionner un fichier"""
        fichier = filedialog.askopenfilename(
            filetypes=[
                ("Fichiers Excel", "*.xlsx"),
                ("Fichiers CSV", "*.csv")
            ]
        )
        if fichier:
            self.fichier_path.set(fichier)

    def predire(self):
        """Effectue les prédictions sur les données"""
        if not self.modeles_charges:
            messagebox.showerror("Erreur", "Les modèles ne sont pas chargés correctement.")
            return
        
        if not self.fichier_path.get():
            messagebox.showwarning("Attention", "Veuillez sélectionner un fichier.")
            return
        
        try:
            # Lecture du fichier
            fichier = self.fichier_path.get()
            df = pd.read_excel(fichier) if fichier.endswith('.xlsx') else pd.read_csv(fichier)
            
            # Vérification des colonnes requises
            colonnes_requises = ['pseudo', 'contexte', 'composant', 'evenement']
            if not all(col in df.columns for col in colonnes_requises):
                messagebox.showerror("Erreur", "Format de fichier incorrect. Colonnes requises : pseudo, contexte, composant, evenement")
                return
            
            # Liste des événements à compter
            # evenements_a_compter = self.selected_features
            
            # Comptage des événements par pseudo
            df_counts = pd.DataFrame()
            for evenement in self.selected_features:
                df_counts[evenement] = df.groupby('pseudo')['evenement'].apply(
                    lambda x: (x == evenement).sum()
                )
            
            df_counts = df_counts.reset_index()
            
            # Préparation des données pour la prédiction
            X = df_counts[evenements_a_compter]
            
            # Utilisation du scaler chargé
            X_scaled = self.scaler_reussite.transform(X)
            
            # Prédictions
            self.predictions_note = np.clip(self.model_note.predict(X_scaled), 0, 20)
            self.predictions_reussite = self.model_reussite.predict(X_scaled)
            self.probas_reussite = self.model_reussite.predict_proba(X_scaled)[:, 1]
            self.pseudos = df_counts['pseudo'].values
            
            # Affichage des résultats
            self.afficher_resultats()
            
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur lors de la prédiction : {str(e)}")

    def afficher_resultats(self):
        """Affiche les résultats dans l'interface"""
        # Nettoyer l'affichage
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Afficher les nouvelles prédictions
        for pseudo, note, reussite, proba in zip(self.pseudos, self.predictions_note, 
                                                self.predictions_reussite, self.probas_reussite):
            self.tree.insert("", "end", values=(
                pseudo,
                f"{note:.2f}",
                f"{'Réussi' if reussite == 1 else 'Échec'} ({proba:.1%})"
            ))
        
        # Mise à jour des statistiques
        if len(self.predictions_note) > 0:
            stats = f"""
    Statistiques des prédictions :

    Date et Heure (UTC) : {self.date_utc}
    Utilisateur : {self.utilisateur}

    • Notes (/20)
    - Moyenne : {np.mean(self.predictions_note):.2f}
    - Médiane : {np.median(self.predictions_note):.2f}
    - Écart-type : {np.std(self.predictions_note):.2f}
    - Note minimale : {np.min(self.predictions_note):.2f}
    - Note maximale : {np.max(self.predictions_note):.2f}
    • Réussite
    - Taux de réussite : {(np.mean(self.predictions_reussite) * 100):.1f}%
    - Probabilité moyenne : {np.mean(self.probas_reussite):.1%}
    • Nombre d'étudiants : {len(self.pseudos)}"""
            self.stats_label.config(text=stats)

    def exporter_resultats(self):
      
        if self.predictions_note is None:
            messagebox.showwarning("Attention", "Aucun résultat à exporter.")
            return
        
        try:
            # Nom de fichier par défaut
            default_filename = f"predictions_{self.date_utc.replace(':', '-')}_{self.utilisateur}.xlsx"
            
            filename = filedialog.asksaveasfilename(
                initialfile=default_filename,
                defaultextension=".xlsx",
                filetypes=[("Fichiers Excel", "*.xlsx")]
            )
            
            if filename:
                # Créer le DataFrame des résultats
                resultats = pd.DataFrame({
                    "Pseudo": self.pseudos,
                    "Note Prédite (/20)": self.predictions_note,
                    "Statut": [f"{'Réussi' if r == 1 else 'Échec'} ({p:.1%})" 
                            for r, p in zip(self.predictions_reussite, self.probas_reussite)]
                })
                
                # Export Excel avec métadonnées
                with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                    resultats.to_excel(writer, sheet_name='Résultats', index=False)
                    
                    metadata = pd.DataFrame({
                        "Information": [
                            "Date et heure (UTC)",
                            "Utilisateur",
                            "Nombre d'étudiants",
                            "Moyenne des notes (/20)",
                            "Note minimale",
                            "Note maximale",
                            "Taux de réussite",
                            "Probabilité moyenne de réussite"
                        ],
                        "Valeur": [
                            self.date_utc,
                            self.utilisateur,
                            len(self.pseudos),
                            f"{np.mean(self.predictions_note):.2f}",
                            f"{np.min(self.predictions_note):.2f}",
                            f"{np.max(self.predictions_note):.2f}",
                            f"{(np.mean(self.predictions_reussite) * 100):.1f}%",
                            f"{np.mean(self.probas_reussite):.1%}"
                        ]
                    })
                    
                    metadata.to_excel(writer, sheet_name='Métadonnées', index=False)
                    
                    # Formatage automatique des colonnes
                    for sheet in writer.sheets:
                        worksheet = writer.sheets[sheet]
                        for column in worksheet.columns:
                            max_length = 0
                            column_letter = column[0].column_letter
                            for cell in column:
                                try:
                                    if len(str(cell.value)) > max_length:
                                        max_length = len(str(cell.value))
                                except:
                                    pass
                                adjusted_width = (max_length + 2)
                                worksheet.column_dimensions[column_letter].width = adjusted_width
                
                messagebox.showinfo("Succès", 
                                f"Résultats exportés avec succès\n"
                                f"Fichier : {filename}")
                
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur lors de l'export : {str(e)}")

    def effacer_resultats(self):
        """Efface tous les résultats"""
        self.fichier_path.set("")
        self.predictions_note = None
        self.predictions_reussite = None
        self.pseudos = None
        
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        self.stats_label.config(text="")

if __name__ == "__main__":
    root = tk.Tk()
    app = SystemePrediction(root)
    root.mainloop()